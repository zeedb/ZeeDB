use crate::art::Art;
use crate::heap::*;
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};

pub struct Storage {
    tables: Vec<Heap>,
    indexes: Vec<Art>,
    sequences: Vec<AtomicI64>,
}

impl Storage {
    pub fn new() -> Self {
        // First 100 tables are reserved for system use.
        let mut tables = Vec::with_capacity(0);
        tables.resize_with(100, Heap::empty);
        // First 100 sequences are reserved for system use.
        let mut sequences = Vec::with_capacity(0);
        sequences.resize_with(100, || AtomicI64::new(0));
        // Bootstrap tables.
        for (table_id, values) in catalog::bootstrap_tables() {
            tables[table_id as usize].insert(&values, 0);
        }
        // Bootstrap sequences.
        for (sequence_id, value) in catalog::bootstrap_sequences() {
            sequences[sequence_id as usize].store(value, Ordering::Relaxed);
        }
        // Initially there are no indexes. TODO index system tables.
        let indexes = vec![];
        Self {
            tables,
            indexes,
            sequences,
        }
    }

    pub fn table(&self, id: i64) -> &Heap {
        &self.tables[id as usize]
    }

    pub fn table_mut(&mut self, id: i64) -> &mut Heap {
        &mut self.tables[id as usize]
    }

    pub fn create_table(&mut self, id: i64) {
        self.tables.resize_with(id as usize + 1, Heap::empty);
    }

    pub fn drop_table(&mut self, id: i64) {
        self.tables[id as usize].truncate()
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

    pub fn table_cardinality(&self, id: i64) -> usize {
        self.table(id).approx_cardinality()
    }

    pub fn column_unique_cardinality(&self, id: i64, column: &String) -> usize {
        match column.as_str() {
            "$xmin" | "$xmax" | "$tid" => self.table_cardinality(id),
            _ => self.table(id).approx_unique_cardinality(column),
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
        }
    }
}
