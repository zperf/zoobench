use crate::error::BenchError;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rand::RngCore;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use zookeeper::{Acl, CreateMode, WatchedEvent, ZkError, ZooKeeper, ZooKeeperExt};

use crate::Cli;

struct LoggingWatcher;

impl zookeeper::Watcher for LoggingWatcher {
    fn handle(&self, event: WatchedEvent) {
        log::info!("Watcher receive new event: {:?}", event);
        // TODO: handle session expired and disconnect event
    }
}

#[derive(Clone, Debug)]
pub struct BenchOption {
    hosts: String,
    timeout: Duration,
    iteration: u32,
    threads: u32,
    ephemeral: bool,
    node_value: Vec<u8>,
    prefix: String,
    node_path_template: String,
    digest: Option<String>,
}

impl From<Cli> for BenchOption {
    fn from(c: Cli) -> Self {
        let mut buf = vec![0; c.node_size];
        rand::thread_rng().fill_bytes(&mut buf);
        BenchOption {
            hosts: c.hosts,
            timeout: c.timeout,
            iteration: c.iteration,
            threads: c.threads,
            ephemeral: c.ephemeral,
            node_value: buf,
            node_path_template: format!("{}/test-node", c.prefix.clone()),
            prefix: c.prefix,
            digest: c.digest,
        }
    }
}

pub struct BenchResult {
    pub elapsed: Duration,
    pub tps: f32,
    pub qps: f32,
}

fn new_progress_style() -> ProgressStyle {
    ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .unwrap()
        .progress_chars("##-")
}

fn skip_last<T>(mut iter: impl Iterator<Item = T>) -> impl Iterator<Item = T> {
    let last = iter.next();
    iter.scan(last, |state, item| std::mem::replace(state, Some(item)))
}

fn prepare(opt: &BenchOption) -> Result<(), anyhow::Error> {
    let zk = ZooKeeper::connect(opt.hosts.as_str(), opt.timeout, LoggingWatcher)?;

    match &opt.digest {
        Some(d) => {
            zk.add_auth("digest", d.to_string().into_bytes())?;
        }
        None => {}
    }

    match zk.delete_recursive(opt.prefix.as_str()) {
        Ok(_) => {}
        Err(e) if e == ZkError::NoNode => {}
        Err(e) => return Err(e.into()),
    }

    let mut s = String::new();
    for p in skip_last(opt.node_path_template.split("/")) {
        if p.is_empty() {
            continue;
        }

        s.push('/');
        s.push_str(p);

        match zk.create(
            s.as_str(),
            Vec::new(),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        ) {
            Ok(_) => {}
            Err(e) if e == ZkError::NodeExists => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

fn do_bench<T>(opt: &BenchOption, bench_fn: T) -> Result<Duration, anyhow::Error>
where
    T: Fn(u32, ProgressBar, &BenchOption) -> Result<(), anyhow::Error> + Send + Sync + Copy,
{
    let bar = MultiProgress::new();
    let start = Instant::now();
    let mut is_err = false;
    thread::scope(|s| {
        let mut threads = Vec::new();
        for tid in 0..opt.threads {
            let pb = bar.add(ProgressBar::new((opt.iteration / opt.threads) as u64));
            pb.set_style(new_progress_style());
            pb.set_message(format!("Worker #{}", tid));
            threads.push(s.spawn(move || bench_fn(tid, pb, opt)));
        }
        for t in threads {
            match t.join().unwrap() {
                Ok(_) => {}
                Err(e) => {
                    is_err = true;
                    log::error!("Worker exit, {}", e);
                }
            }
        }
    });
    let elapsed = start.elapsed();
    if is_err {
        Err(BenchError::BenchFailed().into())
    } else {
        Ok(elapsed)
    }
}

pub fn bench(opt: &BenchOption) -> Result<BenchResult, anyhow::Error> {
    log::info!("Preparing...");
    prepare(opt)?;

    log::info!("Running TPS benchmark");
    let elapsed = do_bench(opt, do_tps_bench)?;
    let tps = opt.iteration as f32 / elapsed.as_secs_f32();

    log::info!("Running QPS benchmark");
    let elapsed = do_bench(opt, do_qps_bench)?;
    let qps = opt.iteration as f32 / elapsed.as_secs_f32();

    Ok(BenchResult { elapsed, tps, qps })
}

fn do_tps_bench(tid: u32, pb: ProgressBar, opt: &BenchOption) -> Result<(), anyhow::Error> {
    let zk = ZooKeeper::connect(opt.hosts.as_str(), opt.timeout, LoggingWatcher)?;
    pb.set_message("Connected");

    let count = opt.iteration / opt.threads;
    for i in tid * count..(tid + 1) * count {
        let path = opt.node_path_template.clone() + i.to_string().as_str();
        let mode = if opt.ephemeral {
            CreateMode::Ephemeral
        } else {
            CreateMode::Persistent
        };
        zk.create(
            path.as_str(),
            opt.node_value.to_vec(),
            Acl::open_unsafe().clone(),
            mode,
        )?;
        pb.inc(1);
        pb.set_message(format!("Created {}", path))
    }

    pb.finish_with_message(format!("Worker #{} finish", tid));
    Ok(())
}

fn do_qps_bench(tid: u32, pb: ProgressBar, opt: &BenchOption) -> Result<(), anyhow::Error> {
    let zk = ZooKeeper::connect(opt.hosts.as_str(), opt.timeout, LoggingWatcher)?;
    pb.set_message("Connected");

    let count = opt.iteration / opt.threads;
    for i in tid * count..(tid + 1) * count {
        let path = opt.node_path_template.clone() + i.to_string().as_str();
        zk.get_data(path.as_str(), false)?;
        pb.inc(1);
        pb.set_message(format!("get_data() {}", path))
    }

    pb.finish_with_message(format!("Worker #{} finish", tid));
    Ok(())
}
