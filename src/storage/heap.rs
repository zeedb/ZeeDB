use crate::page::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use std::collections::HashMap;
use std::fmt;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Arc, Mutex};
use Schema;

// Heap represents a logical table as a list of pages.
// New tuples are added to the end of the heap.
// Deleted tuples are periodically garbage-collected and the heap is compacted.
// During the GC process, the heap is partially sorted on the cluster-by key.
// A good cluster-by key will cluster frequently-updated rows together.
pub struct Heap {
    pages: Vec<Page>,
    counters: HashMap<String, Mutex<Counter>>,
}

type Counter = HyperLogLogPlus<[u8], BuildHasherDefault<ColumnHasher>>;

impl Heap {
    pub fn empty() -> Self {
        Self {
            pages: vec![],
            counters: HashMap::new(),
        }
    }

    pub fn scan(&self) -> Vec<Page> {
        self.pages.iter().map(|page| page.clone()).collect()
    }

    pub fn insert(&mut self, records: &RecordBatch, txn: u64) -> UInt64Array {
        if self.pages.is_empty() {
            self.pages.push(Page::empty(records.schema()));
            for column in records.schema().fields() {
                self.counters.insert(
                    base_name(column.name()).to_string(),
                    Mutex::new(HyperLogLogPlus::new(4, BuildHasherDefault::default()).unwrap()),
                );
            }
        }
        // Allocate arrays to keep track of where we insert the rows.
        let mut tids = UInt64Builder::new(records.num_rows());
        // Insert, adding new pages if needed.
        let mut offset = 0;
        self.insert_more(records, txn, &mut tids, &mut offset);

        tids.finish()
    }

    pub fn insert_more(
        &mut self,
        records: &RecordBatch,
        txn: u64,
        tids: &mut UInt64Builder,
        offset: &mut usize,
    ) {
        // Insert however many records fit in the last page.
        let last = self.pages.last().unwrap();
        last.insert(records, txn, tids, offset);
        // If there are leftover records, add a page and try again.
        if *offset < records.num_rows() {
            self.pages.push(Page::empty(records.schema()));
            self.insert_more(records, txn, tids, offset);
        }
        // Update counters.
        for (i, field) in records.schema().fields().iter().enumerate() {
            match base_name(field.name()) {
                "$xmin" | "$xmax" | "$tid" => {}
                name => {
                    let mut counter = self.counters.get(name).expect(name).lock().unwrap();
                    let column = records.column(i);
                    update_counter(&mut *counter, column);
                }
            }
        }
    }

    pub fn update(&self, pid: usize) -> &Page {
        todo!()
    }

    pub fn truncate(&mut self) {
        self.pages = vec![];
    }

    pub fn approx_cardinality(&self) -> usize {
        self.pages.len() * PAGE_SIZE
    }

    pub fn approx_unique_cardinality(&self, column: &String) -> usize {
        if self.pages.is_empty() {
            return 0;
        }
        self.counters
            .get(column)
            .expect(column)
            .lock()
            .unwrap()
            .count() as usize
    }

    pub fn schema(&self) -> Option<Arc<Schema>> {
        self.pages.first().map(|page| page.schema())
    }

    pub(crate) fn is_uninitialized(&self) -> bool {
        self.pages.is_empty()
    }

    pub fn print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        if let Some(page) = self.pages.last() {
            page.print(f, indent)?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for Heap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.print(f, 0)
    }
}

struct ColumnHasher {
    hash: u64,
}

impl Default for ColumnHasher {
    fn default() -> Self {
        Self {
            hash: 0xcbf29ce484222325,
        }
    }
}

impl Hasher for ColumnHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes.iter() {
            self.hash = self.hash ^ (*byte as u64);
            self.hash = self.hash.wrapping_mul(0x100000001b3);
        }
    }
}

fn update_counter(counter: &mut Counter, column: &Arc<dyn Array>) {
    match column.data_type() {
        DataType::Boolean => update_counter_generic::<BooleanType>(counter, column),
        DataType::Int64 => update_counter_generic::<Int64Type>(counter, column),
        DataType::Float64 => update_counter_generic::<Float64Type>(counter, column),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            update_counter_generic::<TimestampMicrosecondType>(counter, column)
        }
        DataType::Date32(DateUnit::Day) => update_counter_generic::<Date32Type>(counter, column),
        DataType::FixedSizeBinary(16) => todo!(),
        DataType::Utf8 => update_counter_utf8(counter, column),
        other => panic!("type {:?} is not supported", other),
    }
}

fn update_counter_generic<T: ArrowPrimitiveType>(counter: &mut Counter, column: &Arc<dyn Array>) {
    let column: &PrimitiveArray<T> = column.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    for i in 0..column.len() {
        if column.is_valid(i) {
            counter.add(column.value(i).to_byte_slice())
        }
    }
}

fn update_counter_utf8(counter: &mut Counter, column: &Arc<dyn Array>) {
    let column: &StringArray = column.as_any().downcast_ref::<StringArray>().unwrap();
    for i in 0..column.len() {
        if column.is_valid(i) {
            counter.add(column.value(i).as_bytes())
        }
    }
}
