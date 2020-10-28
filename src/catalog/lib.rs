struct CatalogProvider {
    execute: execute::ExecuteProvider,
}

impl CatalogProvider {
    pub fn new() -> Self {
        Self {
            execute: execute::ExecuteProvider::new(),
        }
    }

    pub fn catalog(&mut self, name: &String) -> zetasql::SimpleCatalogProto {
        let mut cat = fixtures::catalog();
        cat.name = Some(name.clone());
        cat.catalog.push(fixtures::metadata());
        cat
    }
}
