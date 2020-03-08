use super::{analyze, catalog};
use std::process::Command;
use tokio::runtime::Runtime;
use zetasql::any_resolved_statement_proto::Node;
use zetasql::{
    AnyResolvedStatementProto, SimpleColumnProto, SimpleTableProto, TypeKind, TypeProto,
};

const SCRIPT: &str = r"
if [[ `docker ps --filter name=test-zetasql-server -q` ]]; then 
    echo 'ZetaSQL server is already running'
    exit 0
fi

stopped=`docker ps --all --filter name=test-zetasql-server -q`

if [[ $stopped ]]; then 
    echo 'ZetaSQL server is stopped'
    docker restart $stopped
    exit 0
fi

docker run \
    --publish 127.0.0.1:50051:50051 \
    --name test-zetasql-server \
    --detach \
    gcr.io/analog-delight-604/zetasql-server@sha256:93ba0c9ad4bbddd50a2b3ed1722fb81f31263afc8c28c23e7e7be61158b0e7ab

until nc -z 127.0.0.1 50051
do
    echo 'waiting for zetasql container...'
    sleep 0.5
done
";

fn create_zetasql_server() {
    Command::new("sh")
        .arg("-c")
        .arg(SCRIPT)
        .output()
        .expect("failed to start docker");
}

#[test]
fn test_create_server() {
    create_zetasql_server();
}

#[tokio::test]
async fn test_analyze() {
    create_zetasql_server();
    match analyze("select 1", 0, catalog()).await {
        Ok((
            _,
            AnyResolvedStatementProto {
                node: Some(Node::ResolvedQueryStmtNode(_)),
            },
        )) => (),
        other => panic!("{:?}", other),
    }
}

#[tokio::test]
async fn test_split() {
    create_zetasql_server();
    let sql = "select 1; select 2";
    let (select1, _) = analyze(sql, 0, catalog()).await.expect("failed to parse");
    assert!(select1 > 0);
    let (select2, _) = analyze(sql, select1, catalog())
        .await
        .expect("failed to parse");
    assert_eq!(select2 as usize, sql.len());
}

#[tokio::test]
async fn test_not_available_fn() {
    create_zetasql_server();
    match analyze("select to_proto(true)", 0, catalog()).await {
        Err(_) => (),
        other => panic!("{:?}", other),
    }
}

#[test]
fn test_sync_analyze() {
    create_zetasql_server();
    let mut runtime = Runtime::new().expect("runtime failed to start");
    match runtime.block_on(analyze("select 1", 0, catalog())) {
        Ok((
            _,
            AnyResolvedStatementProto {
                node: Some(Node::ResolvedQueryStmtNode(_)),
            },
        )) => (),
        other => panic!("{:?}", other),
    }
}

#[test]
fn test_add_table() {
    let table = SimpleTableProto {
        name: Some(String::from("test_table")),
        serialization_id: Some(1),
        column: vec![SimpleColumnProto {
            name: Some(String::from("test_column")),
            r#type: Some(TypeProto {
                type_kind: Some(TypeKind::TypeInt64 as i32),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut catalog = catalog();
    catalog.table.push(table);
}
