use thiserror::Error;

#[derive(Error, Debug)]
pub enum NormError {
    // represents an error in converting to/from the Dtype enum
    #[error("failed to convert value to Dtype")]
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
}

pub type Result<T> = std::result::Result<T, NormError>;
