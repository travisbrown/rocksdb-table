//! Some helpers for working with RocksDB databases.

use rocksdb::{DBCompressionType, DBIterator, IteratorMode, Options, DB};
use std::borrow::Cow;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

pub mod error;
pub mod rework;

/// Marker structs that indicate access mode.
pub mod mode {
    /// Indicates whether a database is opened in read-only or write mode.
    pub trait Mode: 'static {
        fn is_read_only() -> bool;
        fn is_secondary() -> bool;
        fn is_primary() -> bool {
            !Self::is_read_only() && !Self::is_secondary()
        }
    }

    /// Indicates that a database is opened in write mode.
    pub trait IsWriteable: Mode {}

    /// Indicates that a database is opened in secondary mode and can try to catch up with the primary.
    pub trait IsSecondary: Mode {}

    /// Indicates that a database is opened in a mode that only requires a single path (i.e. not
    /// secondary mode).
    pub trait SinglePath: Mode {}

    #[derive(Clone, Copy)]
    pub struct ReadOnly;

    #[derive(Clone, Copy)]
    pub struct Secondary;

    #[derive(Clone, Copy)]
    pub struct Writeable;

    impl Mode for ReadOnly {
        fn is_read_only() -> bool {
            true
        }

        fn is_secondary() -> bool {
            false
        }
    }

    impl Mode for Secondary {
        fn is_read_only() -> bool {
            false
        }

        fn is_secondary() -> bool {
            true
        }
    }

    impl Mode for Writeable {
        fn is_read_only() -> bool {
            false
        }

        fn is_secondary() -> bool {
            false
        }
    }

    impl IsWriteable for Writeable {}
    impl IsSecondary for Secondary {}
    impl SinglePath for ReadOnly {}
    impl SinglePath for Writeable {}
}

/// A wrapper for a RocksDB database that maintains type information about whether it was opened
/// in read-only mode.
#[derive(Clone)]
pub struct Database<M> {
    pub db: Arc<DB>,
    options: Options,
    _mode: PhantomData<M>,
}

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
    fn value_to_bytes(value: &Self::Value) -> Result<Self::ValueBytes, Self::Error>;
    fn index_to_bytes(index: &Self::Index) -> Result<Self::IndexBytes, Self::Error>;

    fn bytes_to_key(bytes: Cow<[u8]>) -> Result<Self::Key, Self::Error>;
    fn bytes_to_value(bytes: Cow<[u8]>) -> Result<Self::Value, Self::Error>;

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
        M: mode::SinglePath,
    {
        Self::open(path, |mut options| {
            if let Some(compression_type) = Self::default_compression_type() {
                options.set_compression_type(compression_type);
            }

            options
        })
    }

    fn open_as_secondary_with_defaults<P: AsRef<Path>, S: AsRef<Path>>(
        path: P,
        secondary_path: S,
    ) -> Result<Self, error::Error>
    where
        M: mode::IsSecondary,
    {
        Self::open_as_secondary(path, secondary_path, |mut options| {
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
        M: mode::SinglePath,
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

    fn open_as_secondary<P: AsRef<Path>, S: AsRef<Path>, F: FnMut(Options) -> Options>(
        path: P,
        secondary_path: S,
        mut options_init: F,
    ) -> Result<Self, error::Error>
    where
        M: mode::IsSecondary,
    {
        let mut options = Options::default();
        options.create_if_missing(true);

        let options = options_init(options);
        let db = DB::open_as_secondary(&options, path.as_ref(), secondary_path.as_ref())?;

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

    fn iter_selected_values<P: Fn(&Self::Key) -> bool>(
        &self,
        pred: P,
    ) -> SelectedValueTableIterator<'_, M, Self, P>
    where
        M: 'static,
    {
        SelectedValueTableIterator {
            underlying: self.database().db.iterator(IteratorMode::Start),
            pred,
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
                Self::bytes_to_value(Cow::from(value_bytes.as_ref())).map(Some)
            })
    }

    fn lookup_index(&self, index: &Self::Index) -> IndexIterator<'_, M, Self>
    where
        M: 'static,
    {
        match Self::index_to_bytes(index) {
            Ok(index_bytes) => IndexIterator::ValidIndex {
                underlying: self.database().db.prefix_iterator(&index_bytes),
                index_bytes,
                _mode: PhantomData,
                _table: PhantomData,
            },
            Err(error) => IndexIterator::InvalidIndex { error: Some(error) },
        }
    }

    fn lookup_index_selected_values<P: Fn(&Self::Key) -> bool>(
        &self,
        index: &Self::Index,
        pred: P,
    ) -> SelectedValueIndexIterator<'_, M, Self, P>
    where
        M: 'static,
    {
        match Self::index_to_bytes(index) {
            Ok(index_bytes) => SelectedValueIndexIterator::ValidIndex {
                underlying: self.database().db.prefix_iterator(&index_bytes),
                index_bytes,
                pred,
                _mode: PhantomData,
                _table: PhantomData,
            },
            Err(error) => SelectedValueIndexIterator::InvalidIndex { error: Some(error) },
        }
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

    fn catch_up_with_primary(&self) -> Result<(), Self::Error>
    where
        M: mode::IsSecondary,
    {
        Ok(self
            .database()
            .db
            .try_catch_up_with_primary()
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
                    T::bytes_to_key(Cow::from(key_bytes.as_ref())).and_then(|key| {
                        T::bytes_to_value(Cow::from(value_bytes.as_ref())).map(|value| (key, value))
                    })
                })
        })
    }
}

/// Allows selection of values to decode (if for example this is expensive).
pub struct SelectedValueTableIterator<'a, M, T, P> {
    underlying: DBIterator<'a>,
    pred: P,
    _mode: PhantomData<M>,
    _table: PhantomData<T>,
}

impl<'a, M: mode::Mode, T: Table<M>, P: Fn(&T::Key) -> bool> Iterator
    for SelectedValueTableIterator<'a, M, T, P>
{
    type Item = Result<(T::Key, Option<T::Value>), T::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|result| {
            result
                .map_err(|error| T::Error::from(error.into()))
                .and_then(|(key_bytes, value_bytes)| {
                    T::bytes_to_key(Cow::from(key_bytes.as_ref())).and_then(|key| {
                        if (self.pred)(&key) {
                            T::bytes_to_value(Cow::from(value_bytes.as_ref()))
                                .map(|value| (key, Some(value)))
                        } else {
                            Ok((key, None))
                        }
                    })
                })
        })
    }
}

pub enum IndexIterator<'a, M, T: Table<M>> {
    ValidIndex {
        underlying: DBIterator<'a>,
        index_bytes: T::IndexBytes,
        _mode: PhantomData<M>,
        _table: PhantomData<T>,
    },
    InvalidIndex {
        error: Option<T::Error>,
    },
}

impl<'a, M: mode::Mode, T: Table<M>> Iterator for IndexIterator<'a, M, T> {
    type Item = Result<(T::Key, T::Value), T::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IndexIterator::ValidIndex {
                underlying,
                index_bytes,
                ..
            } => underlying.next().and_then(|result| match result {
                Ok((key_bytes, value_bytes)) => {
                    if key_bytes.starts_with(index_bytes.as_ref()) {
                        Some(
                            T::bytes_to_key(Cow::from(Vec::from(key_bytes))).and_then(|key| {
                                T::bytes_to_value(Cow::from(Vec::from(value_bytes)))
                                    .map(|value| (key, value))
                            }),
                        )
                    } else {
                        None
                    }
                }
                Err(error) => Some(Err(T::Error::from(error.into()))),
            }),
            IndexIterator::InvalidIndex { error } => error.take().map(Err),
        }
    }
}

/// Allows selection of values to decode (if for example this is expensive).
pub enum SelectedValueIndexIterator<'a, M, T: Table<M>, P> {
    ValidIndex {
        underlying: DBIterator<'a>,
        index_bytes: T::IndexBytes,
        pred: P,
        _mode: PhantomData<M>,
        _table: PhantomData<T>,
    },
    InvalidIndex {
        error: Option<T::Error>,
    },
}

impl<'a, M: mode::Mode, T: Table<M>, P: Fn(&T::Key) -> bool> Iterator
    for SelectedValueIndexIterator<'a, M, T, P>
{
    type Item = Result<(T::Key, Option<T::Value>), T::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SelectedValueIndexIterator::ValidIndex {
                underlying,
                index_bytes,
                pred,
                ..
            } => underlying.next().and_then(|result| match result {
                Ok((key_bytes, value_bytes)) => {
                    if key_bytes.starts_with(index_bytes.as_ref()) {
                        Some(
                            T::bytes_to_key(Cow::from(Vec::from(key_bytes))).and_then(|key| {
                                if (pred)(&key) {
                                    T::bytes_to_value(Cow::from(Vec::from(value_bytes)))
                                        .map(|value| (key, Some(value)))
                                } else {
                                    Ok((key, None))
                                }
                            }),
                        )
                    } else {
                        None
                    }
                }
                Err(error) => Some(Err(T::Error::from(error.into()))),
            }),
            SelectedValueIndexIterator::InvalidIndex { error } => error.take().map(Err),
        }
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

        fn bytes_to_key(bytes: Cow<[u8]>) -> Result<Self::Key, Self::Error> {
            Ok(std::str::from_utf8(bytes.as_ref())?.to_string())
        }

        fn bytes_to_value(bytes: Cow<[u8]>) -> Result<Self::Value, Self::Error> {
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
            &dictionary
                .lookup_index(&"ba".to_string())
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
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
