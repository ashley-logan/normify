use thiserror::Error;

#[derive(Error, Debug)]
pub enum NormError {
    // represents an error in converting to/from the Dtype enum
    #[error("failed to convert value to NormValue")]
    Convert,
    // represents an error in reading from the json file
    #[error("failed to parse json property")]
    Parse,
    // represents a failure to construct the Database object
    #[error("failed to build database")]
    Build,
    // wrapper for the serde json error type
    #[error("{0}")]
    Serde(#[from] serde_json::Error),
    #[error("column type does not match item type")]
    Insert,
    // wrapper for the io error type
    #[error("{0}")]
    IO(#[from] std::io::Error),
    // wrapper for the std::convert error type
    #[error("{0}")]
    TryConvert(#[from] std::convert::Infallible),
}

pub type Result<T> = std::result::Result<T, NormError>;
