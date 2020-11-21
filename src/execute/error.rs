use arrow::error::ArrowError;

#[derive(Debug)]
pub enum Error {
    Arrow(ArrowError),
}

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Self {
        Error::Arrow(e)
    }
}
