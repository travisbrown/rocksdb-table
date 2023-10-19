use rocksdb::merge_operator::MergeFn;
use std::borrow::Cow;

pub trait Entry {
    type Error: From<super::error::Error> + From<rocksdb::Error>;

    type Key;
    type Value;

    type KeyBytes: AsRef<[u8]>;
    type ValueBytes: AsRef<[u8]>;

    fn name() -> Option<&'static str> {
        None
    }

    fn associative_merge() -> Option<(String, &'static dyn MergeFn)> {
        None
    }

    fn new(key: Self::Key, value: Self::Value) -> Self;
    fn key(&self) -> Self::Key;
    fn value(&self) -> Self::Value;

    fn key_to_bytes(key: &Self::Key) -> Result<Self::KeyBytes, Self::Error>;
    fn value_to_bytes(value: &Self::Value) -> Result<Self::ValueBytes, Self::Error>;

    fn bytes_to_key(bytes: Cow<[u8]>) -> Result<Self::Key, Self::Error>;
    fn bytes_to_value(bytes: Cow<[u8]>) -> Result<Self::Value, Self::Error>;
}

pub trait Indexed<const N: usize> {
    type Index;

    const NON_ZERO_N: () = if N == 0 {
        panic!("Non-empty prefix required");
    };

    fn index(&self) -> Self::Index;
    fn index_to_bytes(index: &Self::Index) -> [u8; N];
}
