use rand::Rng;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct Uuid([u8; 16]);

impl Uuid {
    pub fn random() -> Self {
        let mut rng = rand::thread_rng();
        Uuid(rng.gen())
    }

    pub fn serialize(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn deserialize(buffer: &Vec<u8>) -> Self {
        todo!()
    }
}
