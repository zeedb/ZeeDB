use arrow::record_batch::RecordBatch;

pub struct ExecuteProvider {}

impl ExecuteProvider {
    pub fn new() -> Self {
        Self {}
    }
    pub fn execute(
        &mut self,
        sql: &String,
        offset: i32,
        catalog: zetasql::SimpleCatalogProto,
    ) -> RecordBatch {
        todo!()
    }
}
