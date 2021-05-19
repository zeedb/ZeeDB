use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicI64, Ordering},
        Mutex,
    },
};

use context::ContextKey;
use kernel::{AnyArray, Array, DataType, I64Array, RecordBatch, StringArray};
use statistics::TableStatistics;

use crate::{art::Art, heap::*};

pub const STORAGE_KEY: ContextKey<Mutex<Storage>> = ContextKey::new("STORAGE");

pub struct Storage {
    tables: Vec<Heap>,
    indexes: Vec<Art>,
    sequences: Vec<AtomicI64>,
    statistics: HashMap<i64, TableStatistics>,
}

impl Storage {
    pub fn table(&self, id: i64) -> &Heap {
        &self.tables[id as usize]
    }

    pub fn table_mut(&mut self, id: i64) -> &mut Heap {
        &mut self.tables[id as usize]
    }

    pub fn create_table(&mut self, id: i64) {
        self.tables.resize_with(id as usize + 1, Heap::empty);
        self.statistics.insert(id, TableStatistics::default());
    }

    pub fn drop_table(&mut self, id: i64) {
        self.tables[id as usize].truncate();
        self.statistics.remove(&id);
    }

    pub fn index(&self, id: i64) -> &Art {
        &self.indexes[id as usize]
    }

    pub fn index_mut(&mut self, id: i64) -> &mut Art {
        &mut self.indexes[id as usize]
    }

    pub fn create_index(&mut self, id: i64) {
        self.indexes.resize_with(id as usize + 1, Art::empty);
    }

    pub fn drop_index(&mut self, id: i64) {
        self.indexes[id as usize].truncate()
    }

    pub fn next_val(&self, id: i64) -> i64 {
        self.sequences[id as usize].fetch_add(1, Ordering::Relaxed)
    }

    pub fn statistics(&self, id: i64) -> Option<&TableStatistics> {
        self.statistics.get(&id)
    }

    pub fn statistics_mut(&mut self, id: i64) -> Option<&mut TableStatistics> {
        self.statistics.get_mut(&id)
    }
}

impl Default for Storage {
    fn default() -> Self {
        // First 100 tables are reserved for system use.
        let mut tables = Vec::with_capacity(0);
        tables.resize_with(100, Heap::empty);
        // First 100 sequences are reserved for system use.
        let mut sequences = Vec::with_capacity(0);
        sequences.resize_with(100, || AtomicI64::new(0));
        // Bootstrap tables.
        for (table_id, values) in bootstrap_tables() {
            tables[table_id as usize].insert(&values, 0);
        }
        // Bootstrap sequences.
        for (sequence_id, value) in bootstrap_sequences() {
            sequences[sequence_id as usize].store(value, Ordering::Relaxed);
        }
        // Initially there are no indexes.
        let indexes = vec![];
        // Bootstrap statistics.
        let mut statistics = HashMap::new();
        for (table_id, columns) in bootstrap_statistics() {
            let mut table_statistics = TableStatistics::default();
            for (column_name, data_type) in columns {
                table_statistics.create_column(column_name.to_string(), data_type);
            }
            statistics.insert(table_id, table_statistics);
        }
        Self {
            tables,
            indexes,
            sequences,
            statistics,
        }
    }
}

impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Storage:")?;
        for (i, heap) in self.tables.iter().enumerate() {
            if !heap.is_uninitialized() {
                writeln!(f, "\t{}:", i)?;
                heap.print(f, 2)?;
            }
        }
        Ok(())
    }
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        Self {
            tables: self.tables.clone(),
            indexes: self.indexes.clone(),
            sequences: self
                .sequences
                .iter()
                .map(|i| AtomicI64::new(i.load(Ordering::Relaxed)))
                .collect(),
            statistics: self.statistics.clone(),
        }
    }
}

fn bootstrap_tables() -> Vec<(i64, RecordBatch)> {
    vec![(
        5,
        RecordBatch::new(vec![
            (
                "sequence_id".to_string(),
                AnyArray::I64(I64Array::from_values(vec![0, 1, 2])),
            ),
            (
                "sequence_name".to_string(),
                AnyArray::String(StringArray::from_values(vec!["catalog", "table", "index"])),
            ),
        ]),
    )]
}

fn bootstrap_sequences() -> Vec<(i64, i64)> {
    vec![(0, 100), (1, 100), (2, 100)]
}

fn bootstrap_statistics() -> Vec<(i64, Vec<(&'static str, DataType)>)> {
    vec![
        (
            0, // catalog
            vec![
                ("parent_catalog_id", DataType::I64),
                ("catalog_id", DataType::I64),
                ("catalog_name", DataType::String),
            ],
        ),
        (
            1, // table
            vec![
                ("catalog_id", DataType::I64),
                ("table_id", DataType::I64),
                ("table_name", DataType::String),
            ],
        ),
        (
            2, // column
            vec![
                ("table_id", DataType::I64),
                ("column_id", DataType::I64),
                ("column_name", DataType::String),
                ("column_type", DataType::String),
            ],
        ),
        (
            3, // index
            vec![
                ("catalog_id", DataType::I64),
                ("index_id", DataType::I64),
                ("table_id", DataType::I64),
                ("index_name", DataType::String),
            ],
        ),
        (
            4, // index_column
            vec![
                ("index_id", DataType::I64),
                ("column_id", DataType::I64),
                ("index_order", DataType::I64),
            ],
        ),
        (
            5, // sequence
            vec![
                ("sequence_id", DataType::I64),
                ("sequence_name", DataType::String),
            ],
        ),
    ]
}
