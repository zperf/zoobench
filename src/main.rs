#![feature(fn_ptr_trait)]

mod bench;
mod error;

use std::time::Duration;
use bytesize::ByteSize;
use clap::{Parser};
use crate::bench::BenchOption;


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// ZooKeeper hosts
    hosts: String,

    /// Connection timeout
    #[arg(long, short = 't', value_parser = parse_duration, default_value = "10")]
    timeout: Duration,

    /// Number of total znodes
    #[arg(long, short = 'n', default_value_t = 1000)]
    iteration: u32,

    /// Number of threads
    #[arg(long, short = 'j', default_value_t = 8)]
    threads: u32,

    /// ZNode value size in bytes
    #[arg(long, short = 's', value_parser = parse_human_bytes, default_value = "128K")]
    node_size: usize,

    /// Create ephemeral znode or not
    #[arg(long, short, default_value_t = false)]
    ephemeral: bool,

    /// Test prefix
    #[arg(long, short, default_value = "/zoobench")]
    prefix: String,
}

fn parse_human_bytes(arg: &str) -> Result<usize, String> {
    arg.parse::<ByteSize>().map(|x| x.as_u64() as usize)
}

fn parse_duration(arg: &str) -> Result<Duration, std::num::ParseIntError> {
    Ok(Duration::from_secs(arg.parse()?))
}

fn print_bench_result(b: &bench::BenchResult) {
    log::info!("TPS: {:.2}, QPS: {:.2}", b.tps, b.qps);
}

fn main() -> Result<(), anyhow::Error> {
    simple_logger::init_with_level(log::Level::Info).unwrap();

    let cli = Cli::parse();
    dbg!(&cli);
    let option = BenchOption::from(cli);
    let r = bench::bench(&option)?;
    print_bench_result(&r);
    Ok(())
}
