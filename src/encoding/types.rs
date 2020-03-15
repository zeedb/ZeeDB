#[derive(Debug, Clone)]
pub enum Type {
    Bool,
    Int64,
    Double,
    String,
    Bytes,
    Date,
    Timestamp,
    Numeric,
    Struct(Vec<(String, Type)>),
    Array(Box<Type>),
}

impl Type {
    pub fn from(typ: &zetasql::TypeProto) -> Self {
        match typ.type_kind.unwrap() {
            // TypeInt64
            2 => Type::Int64,
            // TypeBool
            5 => Type::Bool,
            // TypeDouble
            7 => Type::Double,
            // TypeString
            8 => Type::String,
            // TypeBytes
            9 => Type::Bytes,
            // TypeDate
            10 => Type::Date,
            // TypeTimestamp
            19 => Type::Timestamp,
            // TypeArray
            16 => {
                let t = typ.clone().array_type.unwrap().element_type.unwrap();
                Type::Array(Box::from(Type::from(&t)))
            }
            // TypeStruct
            17 => {
                let fs = typ.clone().struct_type.unwrap().field;
                Type::Struct(fields(fs))
            }
            // TypeNumeric
            23 => Type::Numeric,
            // Other types
            other => panic!("{:?} not supported", other),
        }
    }
}

fn fields(fs: Vec<zetasql::StructFieldProto>) -> Vec<(String, Type)> {
    let mut list = vec![];
    for f in fs {
        let name = f.field_name.as_ref().unwrap().clone();
        let typ = Type::from(f.field_type.as_ref().unwrap());
        list.push((name, typ))
    }
    list
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
