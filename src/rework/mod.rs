use rocksdb::{
    BoundColumnFamily, DBAccess, DBIteratorWithThreadMode, DBWithThreadMode, IteratorMode,
    MultiThreaded, Options, ThreadMode, TransactionDB,
};
use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

pub mod error;
pub mod iterators;
pub mod mode;

/// A database table.
pub trait Table {
    type Counts;
    type Error: From<error::Error>;
    type Key;
    type KeyBytes: AsRef<[u8]>;
    type Value;
    type ValueBytes: AsRef<[u8]>;

    //fn get_counts(&self) -> Result<Self::Counts, Self::Error>;

    fn key_to_bytes(key: &Self::Key) -> Result<Self::KeyBytes, Self::Error>;
    fn value_to_bytes(value: &Self::Value) -> Result<Self::ValueBytes, Self::Error>;

    fn bytes_to_key(bytes: Cow<[u8]>) -> Result<Self::Key, Self::Error>;
    fn bytes_to_value(bytes: Cow<[u8]>) -> Result<Self::Value, Self::Error>;

    fn prefix_len() -> Option<usize> {
        None
    }
}

pub trait IndexedTable<const N: usize>: Table {
    type Index;

    fn index_to_bytes(index: &Self::Index) -> [u8; N];
}

pub struct NamedTable<T> {
    name: Option<String>,
    _table: PhantomData<T>,
}

impl<T> NamedTable<T> {
    pub fn new() -> Self {
        Self {
            name: None,
            _table: PhantomData,
        }
    }

    pub fn new_cf<'a, S: Into<Cow<'a, str>>>(name: S) -> Self {
        Self {
            name: Some(name.into().to_string()),
            _table: PhantomData,
        }
    }
}

pub trait Db: DBAccess + Sized {
    type CfHandle<'a>
    where
        Self: 'a;

    fn cf_handle(&self, name: &str) -> Option<Self::CfHandle<'_>>;
    fn iterator(&self) -> DBIteratorWithThreadMode<'_, Self>;
    fn prefix_iterator<P: AsRef<[u8]>>(&self, prefix: P) -> DBIteratorWithThreadMode<'_, Self>;

    fn iterator_cf(&self, handle: &Self::CfHandle<'_>) -> DBIteratorWithThreadMode<'_, Self>;
    fn prefix_iterator_cf<P: AsRef<[u8]>>(
        &self,
        handle: &Self::CfHandle<'_>,
        prefix: P,
    ) -> DBIteratorWithThreadMode<'_, Self>;
}

impl Db for DBWithThreadMode<MultiThreaded> {
    type CfHandle<'a> = Arc<BoundColumnFamily<'a>>;

    fn cf_handle(&self, name: &str) -> Option<Self::CfHandle<'_>> {
        self.cf_handle(name)
    }

    fn iterator(&self) -> DBIteratorWithThreadMode<'_, Self> {
        self.iterator(IteratorMode::Start)
    }

    fn prefix_iterator<P: AsRef<[u8]>>(&self, prefix: P) -> DBIteratorWithThreadMode<'_, Self> {
        self.prefix_iterator(prefix)
    }

    fn iterator_cf(&self, handle: &Self::CfHandle<'_>) -> DBIteratorWithThreadMode<'_, Self> {
        self.iterator_cf(handle, IteratorMode::Start)
    }

    fn prefix_iterator_cf<P: AsRef<[u8]>>(
        &self,
        handle: &Self::CfHandle<'_>,
        prefix: P,
    ) -> DBIteratorWithThreadMode<'_, Self> {
        self.prefix_iterator_cf(handle, prefix)
    }
}

impl Db for TransactionDB<MultiThreaded> {
    type CfHandle<'a> = Arc<BoundColumnFamily<'a>>;

    fn cf_handle(&self, name: &str) -> Option<Arc<BoundColumnFamily<'_>>> {
        self.cf_handle(name)
    }

    fn iterator(&self) -> DBIteratorWithThreadMode<'_, Self> {
        self.iterator(IteratorMode::Start)
    }

    fn prefix_iterator<P: AsRef<[u8]>>(&self, prefix: P) -> DBIteratorWithThreadMode<'_, Self> {
        self.prefix_iterator(prefix)
    }

    fn iterator_cf(&self, handle: &Self::CfHandle<'_>) -> DBIteratorWithThreadMode<'_, Self> {
        self.iterator_cf(handle, IteratorMode::Start)
    }

    fn prefix_iterator_cf<P: AsRef<[u8]>>(
        &self,
        handle: &Self::CfHandle<'_>,
        prefix: P,
    ) -> DBIteratorWithThreadMode<'_, Self> {
        self.prefix_iterator_cf(handle, prefix)
    }
}

pub struct Database<M: mode::Mode, D: Db> {
    pub db: Arc<D>,
    _mode: PhantomData<M>,
}

impl<M: mode::Mode, D: Db> Database<M, D> {
    pub fn iter<T: Table>(
        &self,
        table: &NamedTable<T>,
    ) -> Result<iterators::TableIterator<'_, D, T>, error::Error> {
        match &table.name {
            Some(name) => {
                let handle = self
                    .db
                    .cf_handle(name)
                    .ok_or_else(|| error::Error::InvalidCfName(name.clone()))?;

                Ok(iterators::TableIterator::new(self.db.iterator_cf(&handle)))
            }
            None => Ok(iterators::TableIterator::new(self.db.iterator())),
        }
    }

    pub fn iter_index<const N: usize, T: IndexedTable<N>>(
        &self,
        table: &NamedTable<T>,
        index: T::Index,
    ) -> Result<iterators::TableIterator<'_, D, T>, error::Error> {
        match &table.name {
            Some(name) => {
                let handle = self
                    .db
                    .cf_handle(name)
                    .ok_or_else(|| error::Error::InvalidCfName(name.clone()))?;

                Ok(iterators::TableIterator::new(
                    self.db
                        .prefix_iterator_cf(&handle, T::index_to_bytes(&index)),
                ))
            }
            None => Ok(iterators::TableIterator::new(
                self.db.prefix_iterator(T::index_to_bytes(&index)),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};

    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        #[error("RocksDb table error")]
        RocksDbTable(#[from] error::Error),
        #[error("String encoding error")]
        Utf8(#[from] std::str::Utf8Error),
    }

    pub struct Dictionary;

    impl Table for Dictionary {
        type Counts = usize;
        type Error = Error;
        type Key = (u64, DateTime<Utc>);
        type KeyBytes = [u8; 12];
        type Value = u64;
        type ValueBytes = [u8; 8];

        fn key_to_bytes(key: &Self::Key) -> Result<Self::KeyBytes, Self::Error> {
            let mut key_bytes = [0; 12];
            key_bytes[0..8].copy_from_slice(&key.0.to_be_bytes());
            key_bytes[8..12].copy_from_slice(&(key.1.timestamp() as u32).to_be_bytes());
            Ok(key_bytes)
        }

        fn value_to_bytes(value: &Self::Value) -> Result<Self::ValueBytes, Self::Error> {
            Ok(value.to_be_bytes())
        }

        fn bytes_to_key(bytes: Cow<[u8]>) -> Result<Self::Key, Self::Error> {
            let id = u64::from_be_bytes(
                bytes.as_ref()[0..8]
                    .try_into()
                    .map_err(|_| error::Error::InvalidValue(bytes.as_ref().to_vec()))?,
            );

            let timestamp_s = u32::from_be_bytes(
                bytes.as_ref()[8..12]
                    .try_into()
                    .map_err(|_| error::Error::InvalidValue(bytes.as_ref().to_vec()))?,
            );

            let timestamp = Utc
                .timestamp_opt(timestamp_s as i64, 0)
                .single()
                .ok_or_else(|| error::Error::InvalidValue(bytes.as_ref().to_vec()))?;

            Ok((id, timestamp))
        }

        fn bytes_to_value(bytes: Cow<[u8]>) -> Result<Self::Value, Self::Error> {
            Ok(u64::from_be_bytes(
                bytes.as_ref()[0..8]
                    .try_into()
                    .map_err(|_| error::Error::InvalidValue(bytes.as_ref().to_vec()))?,
            ))
        }

        fn prefix_len() -> Option<usize> {
            Some(8)
        }    
    }

    impl IndexedTable<8> for Dictionary {
        type Index = u64;

        fn index_to_bytes(index: &Self::Index) -> [u8; 8] {
            index.to_be_bytes()            
        }
    }

    #[test]
    fn prefix_len() {
        assert_eq!(Dictionary::prefix_len(), Some(8));
    }
}
