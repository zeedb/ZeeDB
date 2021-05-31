use once_cell::sync::OnceCell;
use tokio::runtime::{Handle, Runtime};

pub fn runtime() -> Handle {
    static CACHE: OnceCell<Runtime> = OnceCell::new();
    CACHE
        .get_or_init(|| Runtime::new().unwrap())
        .handle()
        .clone()
}
