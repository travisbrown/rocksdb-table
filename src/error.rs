#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RocksDb error")]
    Db(#[from] rocksdb::Error),
    #[error("Invalid key bytes")]
    InvalidKey(Vec<u8>),
    #[error("Invalid value bytes")]
    InvalidValue(Vec<u8>),
    #[error("Invalid column family name")]
    InvalidCfName(String),
}
