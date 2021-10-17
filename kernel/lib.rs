#![feature(try_trait_v2)]

mod any_array;
mod array_like;
#[macro_use]
mod array_macros;
mod array;
#[cfg(test)]
mod array_tests;
mod bitmask;
mod bool_array;
mod data_type;
#[cfg(test)]
mod data_type_tests;
mod dates;
mod fixed_width;
mod next;
mod primitive_array;
mod record_batch;
#[cfg(test)]
mod record_batch_tests;
mod string_array;

pub use crate::{
    any_array::*, array::*, array_like::*, bitmask::*, bool_array::*, data_type::*, fixed_width::*,
    next::*, primitive_array::*, record_batch::*, string_array::*,
};
