use std::{
    convert::Infallible,
    ops::{ControlFlow, FromResidual, Try},
};

use crate::RecordBatch;

pub enum Next {
    Page(RecordBatch),
    Error(String),
    End,
}

impl Try for Next {
    type Output = RecordBatch;

    type Residual = Option<String>;

    fn from_output(output: Self::Output) -> Self {
        Next::Page(output)
    }

    fn branch(self) -> ControlFlow<Self::Residual, Self::Output> {
        match self {
            Next::Page(record_batch) => ControlFlow::Continue(record_batch),
            Next::Error(message) => ControlFlow::Break(Some(message)),
            Next::End => ControlFlow::Break(None),
        }
    }
}

impl FromResidual<Option<String>> for Next {
    fn from_residual(residual: Option<String>) -> Self {
        match residual {
            Some(message) => Next::Error(message),
            None => Next::End,
        }
    }
}

impl FromResidual<Result<Infallible, String>> for Next {
    fn from_residual(residual: Result<Infallible, String>) -> Self {
        match residual {
            Ok(_) => panic!(),
            Err(message) => Next::Error(message),
        }
    }
}
