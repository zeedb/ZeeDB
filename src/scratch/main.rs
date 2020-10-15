#[derive(Debug)]
struct SearchSpace<T> {
    owned: Vec<T>,
}

struct Ref {
    index: usize,
}

impl<T> SearchSpace<T> {
    fn new() -> Self {
        SearchSpace { owned: vec![] }
    }

    fn add(&mut self, value: T) -> Ref {
        self.owned.push(value);
        Ref {
            index: self.owned.len() - 1,
        }
    }

    fn borrow(&mut self, value: Ref) -> &T {
        &self.owned[value.index]
    }

    fn borrow_mut(&mut self, value: Ref) -> &mut T {
        &mut self.owned[value.index]
    }
}

fn main() {
    let mut ss = SearchSpace::new();
    let a = ss.add(1);
    let b = ss.add(2);
    let c = ss.add(3);
    let ia = ss.borrow_mut(a);
    *ia = *ia + 1;
    println!("{:?}", ss);
}
