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

    pub fn bitmap_scan(&self, mut tids: Vec<u64>, projects: &Vec<String>) -> RecordBatch {
        if tids.is_empty() {
            return self.none(projects);
        }
        // Sort tids so we can access pages in-order.
        tids.sort();
        // Collect tids into 1 batch per page, and scan each page.
        let mut batches = vec![];
        let mut i = 0;
        while i < tids.len() {
            let pid = tids[i] as usize / PAGE_SIZE;
            let mut rids = vec![];
            while i < tids.len() && pid == tids[i] as usize / PAGE_SIZE {
                let rid = tids[i] as usize % PAGE_SIZE;
                rids.push(rid as u32);
                i += 1;
            }
            let batch = self.page(pid).select(projects);
            let map: Arc<dyn Array> = Arc::new(UInt32Array::from(rids));
            batches.push(kernel::gather(&batch, &map));
        }
        // Combine results from each page.
        kernel::cat(&batches)
    }

    fn none(&self, projects: &Vec<String>) -> RecordBatch {
        let schema = self.schema().unwrap();
        let empty_field = |name: &String| match base_name(name) {
            "$xmin" | "$xmax" | "$tid" => Field::new(name, DataType::UInt64, false),
            base_name => {
                let field = schema.field_with_name(base_name).unwrap();
                Field::new(name, field.data_type().clone(), field.is_nullable())
            }
        };
        let fields: Vec<Field> = projects.iter().map(empty_field).collect();
        let columns: Vec<Arc<dyn Array>> = fields.iter().map(|f| empty(f.data_type())).collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    pub fn insert(&mut self, records: &RecordBatch, txn: u64) -> UInt64Array {
        if self.pages.is_empty() {
            self.pages
                .push(Page::empty(self.pages.len(), records.schema()));
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
            self.pages
                .push(Page::empty(self.pages.len(), records.schema()));
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

    pub fn page(&self, pid: usize) -> &Page {
        &self.pages[pid]
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

pub fn empty(as_type: &DataType) -> Arc<dyn Array> {
    match as_type {
        DataType::Boolean => empty_boolean(),
        DataType::Int64 => empty_generic::<Int64Type>(),
        DataType::UInt64 => empty_generic::<UInt64Type>(),
        DataType::Float64 => empty_generic::<Float64Type>(),
        DataType::Date32(DateUnit::Day) => empty_generic::<Date32Type>(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            empty_generic::<TimestampMicrosecondType>()
        }
        DataType::FixedSizeBinary(16) => todo!(),
        DataType::Utf8 => empty_utf8(),
        DataType::Struct(fields) => todo!(),
        DataType::List(element) => todo!(),
        other => panic!("{:?} not supported", other),
    }
}

fn empty_boolean() -> Arc<dyn Array> {
    let mut array = BooleanBuilder::new(0);
    Arc::new(array.finish())
}

fn empty_generic<T: ArrowNumericType>() -> Arc<dyn Array> {
    let mut array = PrimitiveArray::<T>::builder(0);
    Arc::new(array.finish())
}

fn empty_utf8() -> Arc<dyn Array> {
    let mut array = StringBuilder::new(0);
    Arc::new(array.finish())
}
