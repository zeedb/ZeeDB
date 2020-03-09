use super::Type;

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
            Type::Numeric => String::from("NUMERIC"),
            Type::Struct(fields) => {
                let strings: Vec<String> = fields.iter().map(field_to_string).collect();
                format!("STRUCT<{}>", strings.join(", "))
            }
            Type::Array(element) => format!("ARRAY<{}>", element.to_string()),
        }
    }
}

fn field_to_string(field: &(String, Type)) -> String {
    format!("{} {}", field.0, field.1.to_string())
}
