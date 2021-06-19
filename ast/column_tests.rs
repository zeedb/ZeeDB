use kernel::DataType;

use crate::Column;

#[test]
fn test_parse_canonical_name() {
    let examples = vec![
        Column::computed("f", &None, DataType::I64),
        Column::computed("foo", &None, DataType::I64),
        Column::computed("foo", &Some("bar".to_string()), DataType::I64),
        Column::computed("foo.bar", &None, DataType::I64),
    ];
    for c in examples {
        assert_eq!(c.name, Column::parse_canonical_name(&c.canonical_name()));
    }
}
