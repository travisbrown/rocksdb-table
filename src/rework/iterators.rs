use super::Table;
use rocksdb::{DBAccess, DBIteratorWithThreadMode};
use std::borrow::Cow;
use std::marker::PhantomData;

pub struct TableIterator<'a, D: DBAccess, const N: usize, T> {
    underlying: DBIteratorWithThreadMode<'a, D>,
    _table: PhantomData<T>,
}

impl<'a, D: DBAccess, const N: usize, T> TableIterator<'a, D, N, T> {
    pub fn new(underlying: DBIteratorWithThreadMode<'a, D>) -> Self {
        Self {
            underlying,
            _table: PhantomData,
        }
    }
}

impl<'a, D: DBAccess, const N: usize, T: Table<N>> Iterator for TableIterator<'a, D, N, T> {
    type Item = Result<(T::Key, T::Value), T::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|result| {
            result
                .map_err(T::Error::from)
                .and_then(|(key_bytes, value_bytes)| {
                    T::bytes_to_key(Cow::from(key_bytes.as_ref())).and_then(|key| {
                        T::bytes_to_value(Cow::from(value_bytes.as_ref())).map(|value| (key, value))
                    })
                })
        })
    }
}

/// Allows selection of values to decode (if for example this is expensive).
pub struct SelectedValueTableIterator<'a, D: DBAccess, const N: usize, T, P> {
    underlying: DBIteratorWithThreadMode<'a, D>,
    pred: P,
    _table: PhantomData<T>,
}

impl<'a, D: DBAccess, const N: usize, T: Table<N>, P: Fn(&T::Key) -> bool> Iterator
    for SelectedValueTableIterator<'a, D, N, T, P>
{
    type Item = Result<(T::Key, Option<T::Value>), T::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|result| {
            result
                .map_err(T::Error::from)
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

/*
pub struct IndexIterator<'a, D: DBAccess, const N: usize, T: IndexedTable<N>> {
    underlying: DBIteratorWithThreadMode<'a, D>,
    _table: PhantomData<T>,
}

impl<'a, D: DBAccess, T: Table> Iterator for IndexIterator<'a, D, T> {
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
                            T::bytes_to_key(Cow::from(key_bytes.as_ref())).and_then(|key| {
                                T::bytes_to_value(Cow::from(value_bytes.as_ref()))
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
pub enum SelectedValueIndexIterator<'a, D: DBAccess, T: Table, P> {
    ValidIndex {
        underlying: DBIteratorWithThreadMode<'a, D>,
        index_bytes: T::IndexBytes,
        pred: P,
        _table: PhantomData<T>,
    },
    InvalidIndex {
        error: Option<T::Error>,
    },
}

impl<'a, D: DBAccess, T: Table, P: Fn(&T::Key) -> bool> Iterator
    for SelectedValueIndexIterator<'a, D, T, P>
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
                            T::bytes_to_key(Cow::from(key_bytes.as_ref())).and_then(|key| {
                                if (pred)(&key) {
                                    T::bytes_to_value(Cow::from(value_bytes.as_ref()))
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
*/
