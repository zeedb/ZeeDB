use crate::*;
use node::*;
use zetasql::*;

macro_rules! ok {
    ($sql:expr, $expect:expr) => {
        match ParseProvider::new().parse($sql, 0, adventure_works()) {
            Ok((_, found)) => assert_eq!($expect, format!("{}", found)),
            Err(err) => panic!("parse `{}`\n\terror:\t{}\n", $sql, err),
        }
    };
}

fn adventure_works() -> zetasql::SimpleCatalogProto {
    catalog()
}

#[test]
fn test_convert() {
    ok!("SELECT 1", "(LogicalProject [1 $col1] LogicalSingleGet)");
}
