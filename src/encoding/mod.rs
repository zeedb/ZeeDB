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

impl ToString for Type {
    fn to_string(&self) -> String {
        match self {
            Type::Bool => String::from("BOOL"),
            Type::Int64 => String::from("INT64"),
            Type::Double => String::from("DOUBLE"),
            Type::String => String::from("STRING"),
            Type::Bytes => String::from("BYTES"),
            Type::Date => String::from("DATE"),
            Type::Timestamp => String::from("TIMESTAMP"),
            Type::Enum => String::from("ENUM"),
            Type::Numeric => String::from("NUMERIC"),
            Type::Struct(fields) => {
                let strings: Vec<String> = fields.iter().map(field_to_string).collect(); 
                format!("STRUCT<{}>", strings.join(", "))
            },
            Type::Array(element) => format!("ARRAY<{}>", element.to_string()),
        }
    }
}

fn field_to_string(field: &(String, Type)) -> String {
    format!("{} {}", field.0, field.1.to_string())
}

#[test]
fn test_to_string() {
    assert_eq!(String::from("BOOL"), Type::Bool.to_string());
    assert_eq!(String::from("INT64"), Type::Int64.to_string());
    assert_eq!(String::from("DOUBLE"), Type::Double.to_string());
    assert_eq!(String::from("STRING"), Type::String.to_string());
    assert_eq!(String::from("BYTES"), Type::Bytes.to_string());
    assert_eq!(String::from("DATE"), Type::Date.to_string());
    assert_eq!(String::from("TIMESTAMP"), Type::Timestamp.to_string());
    assert_eq!(String::from("ENUM"), Type::Enum.to_string());
    assert_eq!(String::from("NUMERIC"), Type::Numeric.to_string());
    assert_eq!(String::from("STRUCT<i INT64, s STRING>"), Type::Struct(vec!((String::from("i"), Type::Int64), (String::from("s"), Type::String))).to_string());
    assert_eq!(String::from("ARRAY<INT64>"), Type::Array(Box::from(Type::Int64)).to_string());
}