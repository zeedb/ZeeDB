mod string;
mod string_tests;
mod varint64;
mod varint64_tests;

pub enum Type {
    Bool,
    Int64,
    Double,
    String,
    Bytes,
    Date,
    Timestamp,
    Enum,
    Numeric,
    Struct(Vec<(String, Type)>),
    Array(Box<Type>),
}

pub trait Serialize {
    fn serialize(&self, buf: &mut [u8]) -> usize;
}