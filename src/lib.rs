//! Some helpers for working with RocksDB databases.

use ::rocksdb::{DBCompressionType, DBIterator, IteratorMode, Options, DB};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

pub mod error;

/// Some RocksDB exports for convenience.
pub mod rocksdb {
    pub use rocksdb::DBCompressionType;
}

/// Marker structs that indicate access mode.
pub mod mode {
    /// Indicates whether a database is opened in read-only or write mode.
    pub trait Mode: 'static {
        fn is_read_only() -> bool;
    }

    /// Indicates that a database is opened in write mode.
    pub trait IsWriteable {}

    #[derive(Clone, Copy)]
    pub struct ReadOnly;

    #[derive(Clone, Copy)]
    pub struct Writeable;

    impl Mode for ReadOnly {
        fn is_read_only() -> bool {
            true
        }
    }

    impl Mode for Writeable {
        fn is_read_only() -> bool {
            false
        }
    }

    impl IsWriteable for Writeable {}
}

/// A wrapper for a RocksDB database that maintains type information about whether it was opened
/// in read-only mode.
#[derive(Clone)]
pub struct Database<M> {
    pub db: Arc<DB>,
    options: Options,
    _mode: PhantomData<M>,
}

type Pairs<K, V> = Vec<(K, V)>;

/// A database table.
pub trait Table<M>: Sized {
    type Counts;
    type Error: From<error::Error>;
    type Key;
    type KeyBytes: AsRef<[u8]>;
    type Value;
    type ValueBytes: AsRef<[u8]>;
    type Index;
    type IndexBytes: AsRef<[u8]>;

    fn database(&self) -> &Database<M>;
    fn from_database(database: Database<M>) -> Self;
    fn get_counts(&self) -> Result<Self::Counts, Self::Error>;

    fn key_to_bytes(key: &Self::Key) -> Result<Self::KeyBytes, Self::Error>;
    fn value_to_bytes(key: &Self::Value) -> Result<Self::ValueBytes, Self::Error>;
    fn index_to_bytes(index: &Self::Index) -> Result<Self::IndexBytes, Self::Error>;

    fn bytes_to_key<T: AsRef<[u8]>>(bytes: T) -> Result<Self::Key, Self::Error>;
    fn bytes_to_value<T: AsRef<[u8]>>(bytes: T) -> Result<Self::Value, Self::Error>;

    fn default_compression_type() -> Option<DBCompressionType> {
        None
    }

    fn statistics(&self) -> Option<String> {
        self.database().options.get_statistics()
    }

    fn get_estimated_key_count(&self) -> Result<Option<u64>, error::Error> {
        Ok(self
            .database()
            .db
            .property_int_value("rocksdb.estimate-num-keys")?)
    }

    fn open_with_defaults<P: AsRef<Path>>(path: P) -> Result<Self, error::Error>
    where
        M: mode::Mode,
    {
        Self::open(path, |mut options| {
            if let Some(compression_type) = Self::default_compression_type() {
                options.set_compression_type(compression_type);
            }

            options
        })
    }

    fn open<P: AsRef<Path>, F: FnMut(Options) -> Options>(
        path: P,
        mut options_init: F,
    ) -> Result<Self, error::Error>
    where
        M: mode::Mode,
    {
        let mut options = Options::default();
        options.create_if_missing(true);

        let options = options_init(options);

        let db = if M::is_read_only() {
            DB::open_for_read_only(&options, path, true)?
        } else {
            DB::open(&options, path)?
        };

        Ok(Self::from_database(Database {
            db: Arc::new(db),
            options,
            _mode: PhantomData,
        }))
    }

    fn iter(&self) -> TableIterator<'_, M, Self>
    where
        M: 'static,
    {
        TableIterator {
            underlying: self.database().db.iterator(IteratorMode::Start),
            _mode: PhantomData,
            _table: PhantomData,
        }
    }

    fn lookup_key(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let key_bytes = Self::key_to_bytes(key)?;
        self.database()
            .db
            .get_pinned(key_bytes)
            .map_err(error::Error::from)?
            .map_or(Ok(None), |value_bytes| {
                Self::bytes_to_value(value_bytes).map(Some)
            })
    }

    fn lookup_index(
        &self,
        index: &Self::Index,
    ) -> Result<Pairs<Self::Key, Self::Value>, Self::Error> {
        let index_bytes = Self::index_to_bytes(index)?;
        let iterator = self.database().db.prefix_iterator(index_bytes.as_ref());
        let mut pairs = vec![];

        for result in iterator {
            let (key_bytes, value_bytes) = result.map_err(error::Error::from)?;
            if key_bytes.starts_with(index_bytes.as_ref()) {
                let key = Self::bytes_to_key(key_bytes)?;
                let value = Self::bytes_to_value(value_bytes)?;

                pairs.push((key, value));
            } else {
                break;
            }
        }

        Ok(pairs)
    }

    fn put(&self, key: &Self::Key, value: &Self::Value) -> Result<(), Self::Error>
    where
        M: mode::IsWriteable,
    {
        let key_bytes = Self::key_to_bytes(key)?;
        let value_bytes = Self::value_to_bytes(value)?;
        Ok(self
            .database()
            .db
            .put(key_bytes, value_bytes)
            .map_err(error::Error::from)?)
    }
}

pub struct TableIterator<'a, M, T> {
    underlying: DBIterator<'a>,
    _mode: PhantomData<M>,
    _table: PhantomData<T>,
}

impl<'a, M: mode::Mode, T: Table<M>> Iterator for TableIterator<'a, M, T> {
    type Item = Result<(T::Key, T::Value), T::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|result| {
            result
                .map_err(|error| T::Error::from(error.into()))
                .and_then(|(key_bytes, value_bytes)| {
                    T::bytes_to_key(key_bytes)
                        .and_then(|key| T::bytes_to_value(value_bytes).map(|value| (key, value)))
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        #[error("RocksDb table error")]
        RocksDbTable(#[from] error::Error),
        #[error("String encoding error")]
        Utf8(#[from] std::str::Utf8Error),
    }

    struct Dictionary<M> {
        database: Database<M>,
    }

    impl<M: mode::Mode> Table<M> for Dictionary<M> {
        type Counts = usize;
        type Error = Error;
        type Key = String;
        type KeyBytes = Vec<u8>;
        type Value = u64;
        type ValueBytes = [u8; 8];
        type Index = String;
        type IndexBytes = Vec<u8>;

        fn database(&self) -> &Database<M> {
            &self.database
        }

        fn from_database(database: Database<M>) -> Self {
            Self { database }
        }

        fn key_to_bytes(key: &Self::Key) -> Result<Self::KeyBytes, Self::Error> {
            Ok(key.as_bytes().to_vec())
        }

        fn value_to_bytes(value: &Self::Value) -> Result<Self::ValueBytes, Self::Error> {
            Ok(value.to_be_bytes())
        }

        fn index_to_bytes(index: &Self::Index) -> Result<Self::IndexBytes, Self::Error> {
            Ok(index.as_bytes().to_vec())
        }

        fn bytes_to_key<T: AsRef<[u8]>>(bytes: T) -> Result<Self::Key, Self::Error> {
            Ok(std::str::from_utf8(bytes.as_ref())?.to_string())
        }

        fn bytes_to_value<T: AsRef<[u8]>>(bytes: T) -> Result<Self::Value, Self::Error> {
            Ok(u64::from_be_bytes(
                bytes.as_ref()[0..8]
                    .try_into()
                    .map_err(|_| error::Error::InvalidValue(bytes.as_ref().to_vec()))?,
            ))
        }

        fn get_counts(&self) -> Result<Self::Counts, Error> {
            let mut count = 0;

            for result in self.iter() {
                result?;
                count += 1;
            }

            Ok(count)
        }
    }

    fn contents() -> Vec<(String, u64)> {
        vec![
            ("bar", 1000),
            ("baz", 98765),
            ("foo", 1),
            ("abc", 23),
            ("qux", 0),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
    }

    #[test]
    fn lookup_key() {
        let directory = tempfile::tempdir().unwrap();
        let dictionary = Dictionary::<mode::Writeable>::open_with_defaults(directory).unwrap();

        for (key, value) in contents() {
            dictionary.put(&key.to_string(), &value).unwrap();
        }

        assert_eq!(dictionary.lookup_key(&"foo".to_string()).unwrap(), Some(1));
        assert_eq!(
            dictionary.lookup_key(&"bar".to_string()).unwrap(),
            Some(1000)
        );
        assert_eq!(dictionary.lookup_key(&"XYZ".to_string()).unwrap(), None);
    }

    #[test]
    fn lookup_index() {
        let directory = tempfile::tempdir().unwrap();
        let dictionary = Dictionary::<mode::Writeable>::open_with_defaults(directory).unwrap();

        for (key, value) in contents() {
            dictionary.put(&key.to_string(), &value).unwrap();
        }

        assert_eq!(
            &dictionary.lookup_index(&"ba".to_string()).unwrap(),
            &contents()[0..2].to_vec()
        );
    }

    #[test]
    fn iter() {
        let directory = tempfile::tempdir().unwrap();
        let dictionary = Dictionary::<mode::Writeable>::open_with_defaults(directory).unwrap();

        for (key, value) in contents() {
            dictionary.put(&key.to_string(), &value).unwrap();
        }

        let mut expected = contents();
        expected.sort();

        assert_eq!(
            dictionary.iter().collect::<Result<Vec<_>, _>>().unwrap(),
            expected
        );
    }

    #[test]
    fn get_counts() {
        let directory = tempfile::tempdir().unwrap();
        let dictionary = Dictionary::<mode::Writeable>::open_with_defaults(directory).unwrap();

        for (key, value) in contents() {
            dictionary.put(&key.to_string(), &value).unwrap();
        }

        assert_eq!(dictionary.get_counts().unwrap(), contents().len());
    }
}
