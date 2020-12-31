use std::{
    fs, io,
    io::{Read, Write},
};

pub fn read_expected(path: &str) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut expect = String::new();
    file.read_to_string(&mut expect)?;
    Ok(expect)
}

pub fn matches_expected(path: &str, found: String) -> bool {
    match read_expected(path) {
        Ok(expect) if expect == found => true,
        _ => {
            match fs::File::create(path) {
                Ok(mut file) => file.write_all(found.as_bytes()).unwrap(),
                Err(err) => println!("{}: {}", path, err),
            }
            false
        }
    }
}
