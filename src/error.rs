use thiserror::Error;

#[derive(Error, Debug)]
pub enum BenchError {
    #[error("Benchmark failed")]
    BenchFailed(),

    #[error("Unknown error: `{0}`")]
    Unknown(#[from] anyhow::Error),
}
