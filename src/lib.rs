#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

use rocksdb::{ColumnFamilyDescriptor, Options, SliceTransform};
use std::collections::BTreeMap;

use crate::entry::{Entry, Indexed};

pub mod access;
pub mod db;
pub mod entry;
pub mod error;
pub mod iter;
pub mod mode;

#[derive(Clone)]
pub struct DatabaseOptions {
    pub options: Options,
    pub cf_options: BTreeMap<&'static str, Options>,
}

impl DatabaseOptions {
    pub fn column_families(&self) -> Vec<ColumnFamilyDescriptor> {
        self.cf_options
            .iter()
            .map(|(name, options)| ColumnFamilyDescriptor::new(name.to_string(), options.clone()))
            .collect()
    }

    pub fn add<E: Entry>(mut self) -> Self {
        if let Some(name) = E::name() {
            self.cf_options.entry(name).or_default();
        }

        if let Some((merge_name, merge_fn)) = E::associative_merge() {
            if let Some(name) = E::name() {
                let cf_options = self.cf_options.entry(name).or_default();
                cf_options.set_merge_operator_associative(&merge_name, merge_fn);
            } else {
                self.options
                    .set_merge_operator_associative(&merge_name, merge_fn);
            }
        }

        self
    }

    pub fn add_indexed<const N: usize, E: Entry + Indexed<N>>(self) -> Self {
        let mut options = self.add::<E>();

        if let Some(name) = E::name() {
            let cf_options = options.cf_options.entry(name).or_default();
            cf_options.set_prefix_extractor(SliceTransform::create_fixed_prefix(N));
        } else {
            options
                .options
                .set_prefix_extractor(SliceTransform::create_fixed_prefix(N));
        }

        options
    }
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        Self {
            options,
            cf_options: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use derive_quickcheck_arbitrary::Arbitrary;
    use either::Either;
    use itertools::Itertools;
    use std::{borrow::Cow, collections::HashSet};

    use super::*;

    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        #[error("RocksDb error")]
        RocksDb(#[from] rocksdb::Error),
        #[error("RocksDb table error")]
        RocksDbTable(#[from] error::Error),
        #[error("I/O error")]
        Io(#[from] std::io::Error),
        #[error("UTF-8 error")]
        Utf8(#[from] std::string::FromUtf8Error),
    }

    fn sort_and_dedup<E: Entry + Clone>(values: &[E]) -> Vec<E>
    where
        E::Key: Ord,
    {
        let mut values = values.to_vec();
        values.sort_by_key(|score| score.key());

        let mut result = Vec::with_capacity(values.len());

        for (_, group) in &values.into_iter().group_by(|value| value.key()) {
            if let Some(last) = group.last() {
                result.push(last);
            }
        }

        result
    }

    fn filter_left<L, R, E>(result: Result<Either<L, R>, E>) -> Option<Result<L, E>> {
        result.map_or_else(|error| Some(Err(error)), |either| either.left().map(Ok))
    }

    fn simple_table_operations<D: access::Access, M: mode::IsWriteable>(
        db: db::Database<D, M>,
        values: Vec<Simple>,
    ) -> Result<bool, Error> {
        for value in &values {
            db.insert::<Simple>(&value.key(), &value.value())?;
        }

        let sorted_values = sort_and_dedup(&values);
        let read_values = db.iter::<Simple>()?.collect::<Result<Vec<_>, _>>()?;

        Ok(read_values == sorted_values)
    }

    fn indexed_table_operations<D: access::Access, M: mode::IsWriteable>(
        db: db::Database<D, M>,
        scores: Vec<Score>,
    ) -> Result<bool, Error> {
        for score in &scores {
            db.insert::<Score>(&score.key(), &score.value())?;
        }

        let sorted_scores = sort_and_dedup(&scores);
        let read_scores = db.iter::<Score>()?.collect::<Result<Vec<_>, _>>()?;
        let read_selected_scores = db
            .iter_selected::<Score, _>(|_| true)?
            .filter_map(filter_left)
            .collect::<Result<Vec<_>, _>>()?;

        let indices = sorted_scores
            .iter()
            .map(|score| score.index())
            .collect::<HashSet<_>>();

        let index_lookup_matches = indices
            .iter()
            .map(|index| {
                let by_index = sorted_scores
                    .iter()
                    .cloned()
                    .filter(|score| score.index() == *index)
                    .collect::<Vec<_>>();

                let read_by_index = db
                    .lookup_index::<2, Score>(index)?
                    .collect::<Result<Vec<_>, _>>()?;

                let read_selected_by_index = db
                    .lookup_index_selected::<2, Score, _>(index, |_| true)?
                    .filter_map(filter_left)
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(read_by_index == by_index && read_selected_by_index == by_index)
            })
            .all(|result: Result<bool, Error>| result.is_ok_and(|value| value));

        Ok(read_scores == sorted_scores
            && read_selected_scores == sorted_scores
            && index_lookup_matches)
    }

    #[quickcheck]
    fn test_database_simple(values: Vec<Simple>) -> Result<bool, Error> {
        let directory = tempfile::tempdir()?;
        let options = DatabaseOptions::default().add::<Simple>();
        let db: db::Database<_, mode::Writeable> = db::Database::open(directory, options)?;

        simple_table_operations(db, values)
    }

    #[quickcheck]
    fn test_database_simple_transactional(values: Vec<Simple>) -> Result<bool, Error> {
        let directory = tempfile::tempdir()?;
        let options = DatabaseOptions::default().add::<Simple>();
        let db = db::Database::open_transactional(directory, options, Default::default())?;

        simple_table_operations(db, values)
    }

    #[quickcheck]
    fn test_database(scores: Vec<Score>) -> Result<bool, Error> {
        let directory = tempfile::tempdir()?;
        let options = DatabaseOptions::default().add_indexed::<2, Score>();
        let db: db::Database<_, mode::Writeable> = db::Database::open(directory, options)?;

        indexed_table_operations(db, scores)
    }

    #[quickcheck]
    fn test_database_transactional(scores: Vec<Score>) -> Result<bool, Error> {
        let directory = tempfile::tempdir()?;
        let options = DatabaseOptions::default().add_indexed::<2, Score>();
        let db = db::Database::open_transactional(directory, options, Default::default())?;

        indexed_table_operations(db, scores)
    }

    #[derive(Arbitrary, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    pub struct Score {
        id: u16,
        ts: u32,
        value: i64,
    }

    impl Entry for Score {
        type Error = Error;

        type KeyBytes = [u8; 6];
        type ValueBytes = [u8; 8];

        type Key = (u16, u32);
        type Value = i64;

        fn name() -> Option<&'static str> {
            Some("scores")
        }

        fn new(key: Self::Key, value: Self::Value) -> Self {
            Self {
                id: key.0,
                ts: key.1,
                value,
            }
        }

        fn key(&self) -> Self::Key {
            (self.id, self.ts)
        }

        fn value(&self) -> Self::Value {
            self.value
        }

        fn key_to_bytes(key: &Self::Key) -> Result<Self::KeyBytes, Self::Error> {
            let mut key_bytes = [0; 6];
            key_bytes[0..2].copy_from_slice(&key.0.to_be_bytes());
            key_bytes[2..6].copy_from_slice(&key.1.to_be_bytes());
            Ok(key_bytes)
        }

        fn value_to_bytes(value: &Self::Value) -> Result<Self::ValueBytes, Self::Error> {
            Ok(value.to_be_bytes())
        }

        fn bytes_to_key(bytes: Cow<[u8]>) -> Result<Self::Key, Self::Error> {
            let id = u16::from_be_bytes(
                bytes.as_ref()[0..2]
                    .try_into()
                    .map_err(|_| super::error::Error::InvalidValue(bytes.as_ref().to_vec()))?,
            );

            let ts = u32::from_be_bytes(
                bytes.as_ref()[2..6]
                    .try_into()
                    .map_err(|_| super::error::Error::InvalidValue(bytes.as_ref().to_vec()))?,
            );

            Ok((id, ts))
        }

        fn bytes_to_value(bytes: Cow<[u8]>) -> Result<Self::Value, Self::Error> {
            Ok(i64::from_be_bytes(
                bytes.as_ref()[0..8]
                    .try_into()
                    .map_err(|_| super::error::Error::InvalidValue(bytes.as_ref().to_vec()))?,
            ))
        }
    }

    impl Indexed<2> for Score {
        type Index = u16;

        fn index(&self) -> Self::Index {
            self.id
        }

        fn index_to_bytes(index: &Self::Index) -> [u8; 2] {
            index.to_be_bytes()
        }
    }

    #[derive(Arbitrary, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    pub struct Simple {
        key: u16,
        value: String,
    }

    impl Entry for Simple {
        type Error = Error;

        type KeyBytes = [u8; 2];
        type ValueBytes = Vec<u8>;

        type Key = u16;
        type Value = String;

        fn name() -> Option<&'static str> {
            None
        }

        fn new(key: Self::Key, value: Self::Value) -> Self {
            Self { key, value }
        }

        fn key(&self) -> Self::Key {
            self.key
        }

        fn value(&self) -> Self::Value {
            self.value.clone()
        }

        fn key_to_bytes(key: &Self::Key) -> Result<Self::KeyBytes, Self::Error> {
            Ok(key.to_be_bytes())
        }

        fn value_to_bytes(value: &Self::Value) -> Result<Self::ValueBytes, Self::Error> {
            Ok(value.as_bytes().to_vec())
        }

        fn bytes_to_key(bytes: Cow<[u8]>) -> Result<Self::Key, Self::Error> {
            Ok(u16::from_be_bytes(
                bytes.as_ref()[0..2]
                    .try_into()
                    .map_err(|_| super::error::Error::InvalidValue(bytes.as_ref().to_vec()))?,
            ))
        }

        fn bytes_to_value(bytes: Cow<[u8]>) -> Result<Self::Value, Self::Error> {
            Ok(String::from_utf8(bytes.as_ref().to_vec())?)
        }
    }
}
