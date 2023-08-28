use super::error::Error;
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBAccess, DBIteratorWithThreadMode,
    DBWithThreadMode, IteratorMode, MultiThreaded, Options, TransactionDB, TransactionDBOptions,
};
use std::{path::Path, sync::Arc};

pub trait Db: DBAccess + Sized {
    type CfHandle<'a>
    where
        Self: 'a;

    fn open<P: AsRef<Path>>(
        options: &Options,
        cf_descriptors: Vec<ColumnFamilyDescriptor>,
        path: P,
    ) -> Result<Self, Error>;
    fn open_read_only<P: AsRef<Path>>(
        options: &Options,
        cf_descriptors: Vec<ColumnFamilyDescriptor>,
        path: P,
    ) -> Result<Self, Error>;

    fn cf_handle(&self, name: &str) -> Option<Self::CfHandle<'_>>;
    fn iterator(&self) -> DBIteratorWithThreadMode<'_, Self>;
    fn prefix_iterator<P: AsRef<[u8]>>(&self, prefix: P) -> DBIteratorWithThreadMode<'_, Self>;

    fn iterator_cf(&self, handle: &Self::CfHandle<'_>) -> DBIteratorWithThreadMode<'_, Self>;
    fn prefix_iterator_cf<P: AsRef<[u8]>>(
        &self,
        handle: &Self::CfHandle<'_>,
        prefix: P,
    ) -> DBIteratorWithThreadMode<'_, Self>;

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<(), rocksdb::Error>;
    fn put_cf<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        handle: &Self::CfHandle<'_>,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error>;

    fn merge<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V)
        -> Result<(), rocksdb::Error>;
    fn merge_cf<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        handle: &Self::CfHandle<'_>,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error>;
}

impl Db for DBWithThreadMode<MultiThreaded> {
    type CfHandle<'a> = Arc<BoundColumnFamily<'a>>;

    fn open<P: AsRef<Path>>(
        options: &Options,
        cf_descriptors: Vec<ColumnFamilyDescriptor>,
        path: P,
    ) -> Result<Self, Error> {
        if cf_descriptors.is_empty() {
            Self::open(options, path).map_err(Error::from)
        } else {
            Self::open_cf_descriptors(options, path, cf_descriptors).map_err(Error::from)
        }
    }

    fn open_read_only<P: AsRef<Path>>(
        options: &Options,
        cf_descriptors: Vec<ColumnFamilyDescriptor>,
        path: P,
    ) -> Result<Self, Error> {
        if cf_descriptors.is_empty() {
            Self::open_for_read_only(options, path, true).map_err(Error::from)
        } else {
            Self::open_cf_descriptors_read_only(options, path, cf_descriptors, true)
                .map_err(Error::from)
        }
    }

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

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<(), rocksdb::Error> {
        self.put(key, value)
    }

    fn put_cf<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        handle: &Self::CfHandle<'_>,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error> {
        self.put_cf(handle, key, value)
    }

    fn merge<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error> {
        self.merge(key, value)
    }

    fn merge_cf<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        handle: &Self::CfHandle<'_>,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error> {
        self.merge_cf(handle, key, value)
    }
}

impl Db for TransactionDB<MultiThreaded> {
    type CfHandle<'a> = Arc<BoundColumnFamily<'a>>;

    fn open<P: AsRef<Path>>(
        options: &Options,
        cf_descriptors: Vec<ColumnFamilyDescriptor>,
        path: P,
    ) -> Result<Self, Error> {
        if cf_descriptors.is_empty() {
            Self::open(options, &TransactionDBOptions::default(), path).map_err(Error::from)
        } else {
            Self::open_cf_descriptors(
                options,
                &TransactionDBOptions::default(),
                path,
                cf_descriptors,
            )
            .map_err(Error::from)
        }
    }

    fn open_read_only<P: AsRef<Path>>(
        options: &Options,
        cf_descriptors: Vec<ColumnFamilyDescriptor>,
        path: P,
    ) -> Result<Self, Error> {
        <Self as Db>::open(options, cf_descriptors, path)
    }

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

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<(), rocksdb::Error> {
        self.put(key, value)
    }

    fn put_cf<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        handle: &Self::CfHandle<'_>,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error> {
        self.put_cf(handle, key, value)
    }

    fn merge<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error> {
        self.merge(key, value)
    }

    fn merge_cf<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        handle: &Self::CfHandle<'_>,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error> {
        self.merge_cf(handle, key, value)
    }
}
