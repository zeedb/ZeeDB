use arrow::datatypes::*;

pub fn to_proto(data: &DataType) -> zetasql::TypeProto {
    match data {
        DataType::Int64 => zetasql::TypeProto {
            type_kind: Some(2),
            ..Default::default()
        },
        DataType::Boolean => zetasql::TypeProto {
            type_kind: Some(5),
            ..Default::default()
        },
        DataType::Float64 => zetasql::TypeProto {
            type_kind: Some(7),
            ..Default::default()
        },
        DataType::Utf8 => zetasql::TypeProto {
            type_kind: Some(8),
            ..Default::default()
        },
        DataType::Date32(DateUnit::Day) => zetasql::TypeProto {
            type_kind: Some(10),
            ..Default::default()
        },
        DataType::Timestamp(TimeUnit::Microsecond, None) => zetasql::TypeProto {
            type_kind: Some(19),
            ..Default::default()
        },
        DataType::FixedSizeBinary(16) => zetasql::TypeProto {
            type_kind: Some(23),
            ..Default::default()
        },
        DataType::Struct(fields) => todo!(),
        DataType::List(element) => todo!(),
        other => panic!("{:?} not supported", other),
    }
}

pub fn from_proto(column_type: &zetasql::TypeProto) -> DataType {
    match column_type.type_kind.unwrap() {
        2 => DataType::Int64,
        5 => DataType::Boolean,
        7 => DataType::Float64,
        8 => DataType::Utf8,
        10 => DataType::Date32(DateUnit::Day),
        19 => DataType::Timestamp(TimeUnit::Microsecond, None),
        16 => {
            let t = column_type
                .clone()
                .array_type
                .unwrap()
                .element_type
                .unwrap();
            DataType::List(Box::from(from_proto(&t)))
        }
        17 => {
            let fs = column_type.clone().struct_type.unwrap().field;
            DataType::Struct(fields(fs))
        }
        23 => DataType::FixedSizeBinary(16),
        other => panic!("{:?} not supported", other),
    }
}

fn fields(fs: Vec<zetasql::StructFieldProto>) -> Vec<Field> {
    let mut list = vec![];
    for f in fs {
        list.push(Field::new(
            f.field_name.unwrap().as_str(),
            from_proto(&f.field_type.unwrap()),
            false,
        ))
    }
    list
}

pub fn to_string(data: &DataType) -> String {
    match data {
        DataType::Boolean => "BOOL".to_string(),
        DataType::Int64 => "INT64".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 => "STRING".to_string(),
        DataType::Date32(DateUnit::Day) => "DATE".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP".to_string(),
        DataType::FixedSizeBinary(16) => "NUMERIC".to_string(),
        DataType::Struct(fields) => {
            let strings: Vec<String> = fields
                .iter()
                .map(|field| format!("{} {}", field.name(), to_string(field.data_type())))
                .collect();
            format!("STRUCT<{}>", strings.join(", "))
        }
        DataType::List(element) => format!("ARRAY<{}>", to_string(&element)),
        other => panic!("{:?}", other),
    }
}

pub fn from_string(string: &str) -> DataType {
    match string {
        "BOOL" => DataType::Boolean,
        "INT64" => DataType::Int64,
        "DOUBLE" => DataType::Float64,
        "STRING" => DataType::Utf8,
        "DATE" => DataType::Date32(DateUnit::Day),
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "NUMERIC" => DataType::FixedSizeBinary(16),
        _ if string.starts_with("STRUCT") => todo!(),
        _ if string.starts_with("ARRAY") => todo!(),
        other => panic!("{:?}", other),
    }
}
