use std::fs;
use std::io;
use std::io::{Read, Write};
use zetasql::*;

pub fn read_expected(path: &String) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut expect = String::new();
    file.read_to_string(&mut expect)?;
    Ok(expect)
}

pub fn matches_expected(path: &String, found: String) -> bool {
    match read_expected(path) {
        Ok(expect) if expect == found => true,
        _ => {
            let mut file = fs::File::create(path).unwrap();
            file.write_all(found.as_bytes()).unwrap();
            false
        }
    }
}
