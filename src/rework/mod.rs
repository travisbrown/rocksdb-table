use rocksdb::merge_operator::MergeFn;
use rocksdb::{ColumnFamilyDescriptor, Options, SliceTransform};
use std::borrow::Cow;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

pub mod db;
pub mod error;
pub mod iterators;
pub mod mode;

/// A database table.
pub trait Table<const N: usize> {
    type Counts;
    type Error: From<error::Error> + From<rocksdb::Error>;
    type Key;
    type KeyBytes: AsRef<[u8]>;
    type Value;
    type ValueBytes: AsRef<[u8]>;
    type Index;

    const NON_ZERO_N: () = if N == 0 {
        panic!("Non-empty prefix required");
    };

    fn key_to_bytes(key: &Self::Key) -> Result<Self::KeyBytes, Self::Error>;
    fn value_to_bytes(value: &Self::Value) -> Result<Self::ValueBytes, Self::Error>;

    fn bytes_to_key(bytes: Cow<[u8]>) -> Result<Self::Key, Self::Error>;
    fn bytes_to_value(bytes: Cow<[u8]>) -> Result<Self::Value, Self::Error>;

    fn index_to_bytes(index: &Self::Index) -> [u8; N];
    fn associative_merge() -> Option<(String, &'static dyn MergeFn)> {
        None
    }

    fn prefix_len() -> usize {
        N
    }

    fn configure_options(options: &mut Options) {
        if Self::prefix_len() > 0 {
            options.set_prefix_extractor(SliceTransform::create_fixed_prefix(Self::prefix_len()));
        }

        if let Some((merge_name, merge_fn)) = Self::associative_merge() {
            options.set_merge_operator_associative(&merge_name, merge_fn);
        }
    }
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

    pub fn cf_descriptor<const N: usize>(&self) -> Option<ColumnFamilyDescriptor>
    where
        T: Table<N>,
    {
        self.cf_descriptor_opt(|_| ())
    }

    pub fn cf_descriptor_opt<F: Fn(&mut Options), const N: usize>(
        &self,
        configure: F,
    ) -> Option<ColumnFamilyDescriptor>
    where
        T: Table<N>,
    {
        self.name.as_ref().map(|name| {
            let mut options = Options::default();
            T::configure_options(&mut options);
            configure(&mut options);

            ColumnFamilyDescriptor::new(name, options)
        })
    }
}

pub enum TableConfig {
    WithCfs {
        cf_descriptors: Vec<ColumnFamilyDescriptor>,
    },
    WithoutCfs {
        options: Options,
    },
}

impl TableConfig {
    pub fn from_table_1<const N1: usize, T1: Table<N1>>(table_1: &NamedTable<T1>) -> Self {
        match table_1.cf_descriptor() {
            Some(cf_descriptor_1) => Self::WithCfs {
                cf_descriptors: vec![cf_descriptor_1],
            },
            None => {
                let mut options = Options::default();
                T1::configure_options(&mut options);

                Self::WithoutCfs { options }
            }
        }
    }

    fn parts(self) -> (Options, Vec<ColumnFamilyDescriptor>) {
        match self {
            Self::WithCfs { cf_descriptors } => (Options::default(), cf_descriptors),
            Self::WithoutCfs { options } => (options, vec![]),
        }
    }
}

pub struct Database<M: mode::Mode, D: db::Db> {
    pub db: Arc<D>,
    _mode: PhantomData<M>,
}

impl<M: mode::Mode, D: db::Db> Database<M, D> {
    pub fn open<P: AsRef<Path>>(config: TableConfig, path: P) -> Result<Self, error::Error> {
        let (mut base_options, cf_descriptors) = config.parts();
        base_options.create_if_missing(true);
        base_options.create_missing_column_families(!cf_descriptors.is_empty());

        let db = match M::mode_type() {
            mode::ModeType::Writeable => D::open(&base_options, cf_descriptors, path),
            mode::ModeType::ReadOnly => D::open_read_only(&base_options, cf_descriptors, path),
        }?;

        Ok(Self {
            db: Arc::new(db),
            _mode: PhantomData,
        })
    }

    pub fn iter<const N: usize, T: Table<N>>(
        &self,
        table: &NamedTable<T>,
    ) -> Result<iterators::TableIterator<'_, D, N, T>, error::Error> {
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

    pub fn iter_index<const N: usize, T: Table<N>>(
        &self,
        table: &NamedTable<T>,
        index: T::Index,
    ) -> Result<iterators::TableIterator<'_, D, N, T>, error::Error> {
        // Verify that the prefix length is non-zero.
        let _ = T::NON_ZERO_N;

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

impl<D: db::Db> Database<mode::Writeable, D> {
    pub fn put<const N: usize, T: Table<N>>(
        &self,
        table: &NamedTable<T>,
        key: &T::Key,
        value: &T::Value,
    ) -> Result<(), T::Error> {
        let key_bytes = T::key_to_bytes(key)?;
        let value_bytes = T::value_to_bytes(value)?;

        match &table.name {
            Some(name) => {
                let handle = self
                    .db
                    .cf_handle(name)
                    .ok_or_else(|| error::Error::InvalidCfName(name.clone()))?;

                self.db
                    .put_cf(&handle, key_bytes, value_bytes)
                    .map_err(T::Error::from)
            }
            None => self.db.put(key_bytes, value_bytes).map_err(T::Error::from),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use rocksdb::{DBWithThreadMode, MultiThreaded, MergeOperands};

    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        #[error("RocksDb error")]
        RocksDb(#[from] rocksdb::Error),
        #[error("RocksDb table error")]
        RocksDbTable(#[from] error::Error),
        #[error("String encoding error")]
        Utf8(#[from] std::str::Utf8Error),
    }

    pub struct CountsDb;

    impl Table<8> for CountsDb {
        type Counts = usize;
        type Error = Error;
        type Key = (u64, DateTime<Utc>);
        type KeyBytes = [u8; 12];
        type Value = u32;
        type ValueBytes = [u8; 4];
        type Index = u64;

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
            Ok(u32::from_be_bytes(
                bytes.as_ref()[0..4]
                    .try_into()
                    .map_err(|_| error::Error::InvalidValue(bytes.as_ref().to_vec()))?,
            ))
        }

        fn index_to_bytes(index: &Self::Index) -> [u8; 8] {
            index.to_be_bytes()
        }

        fn associative_merge() -> Option<(String, &'static dyn MergeFn)> {
            Some(("addition".to_string(), &add_u64))
        }
    }

    fn add_u64(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut sum: u32 = 0;
        existing_val.map(|value| {
            let bytes: Result<[u8; 4], _> = value[0..4].try_into();
            sum += u32::from_be_bytes(bytes.unwrap());
        });

        for value in operands {
            let bytes: Result<[u8; 4], _> = value[0..4].try_into();
            sum += u32::from_be_bytes(bytes.unwrap());
        }
        Some(sum.to_be_bytes().to_vec())
    }

    #[test]
    fn db() {
        let directory = tempfile::tempdir().unwrap();
        let table = NamedTable::<CountsDb>::new_cf("foo");
        let config = TableConfig::from_table_1(&table);
        let database = Database::<mode::Writeable, DBWithThreadMode<MultiThreaded>>::open(
            config,
            directory.path(),
        )
        .unwrap();

        let values = vec![
            (
                (123, Utc.timestamp_opt(1693225042, 0).single().unwrap()),
                100,
            ),
            (
                (456, Utc.timestamp_opt(1693222131, 0).single().unwrap()),
                200,
            ),
        ];

        for ((id, timestamp), count) in &values {
            database.put(&table, &(*id, *timestamp), count).unwrap();
        }

        let read_values = database
            .iter(&table)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(read_values, values);

        /*
        let dictionary = Dictionary::<mode::Writeable>::open_with_defaults(directory).unwrap();

        for (key, value) in contents() {
            dictionary.put(&key.to_string(), &value).unwrap();
        }

        assert_eq!(dictionary.lookup_key(&"foo".to_string()).unwrap(), Some(1));
        assert_eq!(
            dictionary.lookup_key(&"bar".to_string()).unwrap(),
            Some(1000)
        );
        assert_eq!(dictionary.lookup_key(&"XYZ".to_string()).unwrap(), None);*/
    }
}
