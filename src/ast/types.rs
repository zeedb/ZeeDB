use std::fmt;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
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

impl Type {
    pub fn from(typ: &zetasql::TypeProto) -> Self {
        match typ.type_kind.unwrap() {
            2 => Type::Int64,
            5 => Type::Bool,
            7 => Type::Double,
            8 => Type::String,
            9 => Type::Bytes,
            10 => Type::Date,
            19 => Type::Timestamp,
            16 => {
                let t = typ.clone().array_type.unwrap().element_type.unwrap();
                Type::Array(Box::from(Type::from(&t)))
            }
            17 => {
                let fs = typ.clone().struct_type.unwrap().field;
                Type::Struct(fields(fs))
            }
            23 => Type::Numeric,
            other => panic!("{:?} not supported", other),
        }
    }

    pub fn type_proto(&self) -> zetasql::TypeProto {
        match self {
            Type::Int64 => zetasql::TypeProto {
                type_kind: Some(2),
                ..Default::default()
            },
            Type::Bool => zetasql::TypeProto {
                type_kind: Some(5),
                ..Default::default()
            },
            Type::Double => zetasql::TypeProto {
                type_kind: Some(7),
                ..Default::default()
            },
            Type::String => zetasql::TypeProto {
                type_kind: Some(8),
                ..Default::default()
            },
            Type::Bytes => zetasql::TypeProto {
                type_kind: Some(9),
                ..Default::default()
            },
            Type::Date => zetasql::TypeProto {
                type_kind: Some(10),
                ..Default::default()
            },
            Type::Timestamp => zetasql::TypeProto {
                type_kind: Some(19),
                ..Default::default()
            },
            _ => todo!(),
        }
    }

    pub fn parse(string: &str) -> Type {
        match string {
            "BOOL" => Type::Bool,
            "INT64" => Type::Int64,
            "DOUBLE" => Type::Double,
            "STRING" => Type::String,
            "BYTES" => Type::Bytes,
            "DATE" => Type::Date,
            "TIMESTAMP" => Type::Timestamp,
            "ENUM" => Type::Enum,
            "NUMERIC" => Type::Numeric,
            _ => todo!(),
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

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Bool => write!(f, "BOOL"),
            Type::Int64 => write!(f, "INT64"),
            Type::Double => write!(f, "DOUBLE"),
            Type::String => write!(f, "STRING"),
            Type::Bytes => write!(f, "BYTES"),
            Type::Date => write!(f, "DATE"),
            Type::Timestamp => write!(f, "TIMESTAMP"),
            Type::Enum => write!(f, "ENUM"),
            Type::Numeric => write!(f, "NUMERIC"),
            Type::Struct(fields) => {
                let strings: Vec<String> = fields.iter().map(field_to_string).collect();
                write!(f, "STRUCT<{}>", strings.join(", "))
            }
            Type::Array(element) => write!(f, "ARRAY<{}>", element.to_string()),
        }
    }
}

fn field_to_string(field: &(String, Type)) -> String {
    format!("{} {}", field.0, field.1.to_string())
}
