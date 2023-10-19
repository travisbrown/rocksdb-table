use either::Either;
use rocksdb::{DBAccess, DBIteratorWithThreadMode};
use std::{borrow::Cow, marker::PhantomData};

use crate::entry::Entry;

pub struct EntryIterator<'a, D: DBAccess, E> {
    underlying: DBIteratorWithThreadMode<'a, D>,
    _entry: PhantomData<E>,
}

impl<'a, D: DBAccess, E> EntryIterator<'a, D, E> {
    pub fn new(underlying: DBIteratorWithThreadMode<'a, D>) -> Self {
        Self {
            underlying,
            _entry: PhantomData,
        }
    }
}

impl<'a, D: DBAccess, E: Entry> Iterator for EntryIterator<'a, D, E> {
    type Item = Result<E, E::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|result| {
            result
                .map_err(E::Error::from)
                .and_then(|(key_bytes, value_bytes)| {
                    E::bytes_to_key(Cow::from(key_bytes.as_ref())).and_then(|key| {
                        E::bytes_to_value(Cow::from(value_bytes.as_ref()))
                            .map(|value| E::new(key, value))
                    })
                })
        })
    }
}

/// Allows selection of values to decode (if for example this is expensive).
pub struct SelectedEntryIterator<'a, D: DBAccess, E, P> {
    underlying: DBIteratorWithThreadMode<'a, D>,
    pred: P,
    _entry: PhantomData<E>,
}

impl<'a, D: DBAccess, E, P> SelectedEntryIterator<'a, D, E, P> {
    pub fn new(underlying: DBIteratorWithThreadMode<'a, D>, pred: P) -> Self {
        Self {
            underlying,
            pred,
            _entry: PhantomData,
        }
    }
}

impl<'a, D: DBAccess, E: Entry, P: Fn(&E::Key) -> bool> Iterator
    for SelectedEntryIterator<'a, D, E, P>
{
    type Item = Result<Either<E, E::Key>, E::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|result| {
            result
                .map_err(E::Error::from)
                .and_then(|(key_bytes, value_bytes)| {
                    E::bytes_to_key(Cow::from(key_bytes.as_ref())).and_then(|key| {
                        if (self.pred)(&key) {
                            E::bytes_to_value(Cow::from(value_bytes.as_ref()))
                                .map(|value| Either::Left(E::new(key, value)))
                        } else {
                            Ok(Either::Right(key))
                        }
                    })
                })
        })
    }
}
