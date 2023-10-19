use rocksdb::{DBAccess, DBWithThreadMode, IteratorMode, SingleThreaded, TransactionDB};
use std::{borrow::Cow, sync::Arc};

use crate::{
    entry::{Entry, Indexed},
    error::Error,
    iter::{EntryIterator, SelectedEntryIterator},
};

pub trait Access: Sized {
    type Db: DBAccess;

    fn lookup_entry<E: Entry>(&self, key: &E::Key) -> Result<Option<E::Value>, E::Error>;
    fn lookup_entries<E: Entry, I: IntoIterator<Item = E::Key>>(
        &self,
        keys: I,
    ) -> Result<Vec<Option<E::Value>>, E::Error>;
    fn lookup_entries_by_index<const N: usize, E: Entry + Indexed<N>>(
        &self,
        index: &E::Index,
    ) -> Result<EntryIterator<Self::Db, E>, Error>;
    fn lookup_selected_entries_by_index<
        const N: usize,
        E: Entry + Indexed<N>,
        P: Fn(&E::Key) -> bool,
    >(
        &self,
        index: &E::Index,
        pred: P,
    ) -> Result<SelectedEntryIterator<Self::Db, E, P>, Error>;
    fn iter_entries<E: Entry>(&self) -> Result<EntryIterator<Self::Db, E>, Error>;
    fn iter_selected_entries<E: Entry, P: Fn(&E::Key) -> bool>(
        &self,
        pred: P,
    ) -> Result<SelectedEntryIterator<Self::Db, E, P>, Error>;

    fn insert<E: Entry>(&self, key: &E::Key, value: &E::Value) -> Result<(), E::Error>;
}

impl Access for DBWithThreadMode<SingleThreaded> {
    type Db = Self;

    fn lookup_entry<E: Entry>(&self, key: &E::Key) -> Result<Option<E::Value>, E::Error> {
        let key_bytes = E::key_to_bytes(key)?;

        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| E::Error::from(Error::InvalidCfName(name.to_string())))?;

                self.get_pinned_cf(column_family, key_bytes)
                    .map_err(E::Error::from)?
                    .map_or(Ok(None), |value_bytes| {
                        E::bytes_to_value(Cow::from(value_bytes.as_ref())).map(Some)
                    })
            }
            None => self
                .get_pinned(key_bytes)
                .map_err(E::Error::from)?
                .map_or(Ok(None), |value_bytes| {
                    E::bytes_to_value(Cow::from(value_bytes.as_ref())).map(Some)
                }),
        }
    }

    fn lookup_entries<E: Entry, I: IntoIterator<Item = E::Key>>(
        &self,
        keys: I,
    ) -> Result<Vec<Option<E::Value>>, E::Error> {
        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| E::Error::from(Error::InvalidCfName(name.to_string())))?;

                let keys_bytes = keys
                    .into_iter()
                    .map(|key| E::key_to_bytes(&key).map(|bytes| (column_family, bytes)))
                    .collect::<Result<Vec<_>, _>>()?;

                self.multi_get_cf(keys_bytes)
                    .into_iter()
                    .map(|result| {
                        result
                            .map_err(E::Error::from)?
                            .map_or(Ok(None), |value_bytes| {
                                E::bytes_to_value(Cow::from(value_bytes)).map(Some)
                            })
                    })
                    .collect()
            }
            None => {
                let keys_bytes = keys
                    .into_iter()
                    .map(|key| E::key_to_bytes(&key))
                    .collect::<Result<Vec<_>, _>>()?;

                self.multi_get(keys_bytes)
                    .into_iter()
                    .map(|result| {
                        result
                            .map_err(E::Error::from)?
                            .map_or(Ok(None), |value_bytes| {
                                E::bytes_to_value(Cow::from(value_bytes)).map(Some)
                            })
                    })
                    .collect()
            }
        }
    }

    fn lookup_entries_by_index<const N: usize, E: Entry + Indexed<N>>(
        &self,
        index: &E::Index,
    ) -> Result<EntryIterator<Self::Db, E>, Error> {
        let index_bytes = E::index_to_bytes(index);

        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(EntryIterator::new(
                    self.prefix_iterator_cf(column_family, index_bytes),
                ))
            }
            None => Ok(EntryIterator::new(self.prefix_iterator(index_bytes))),
        }
    }

    fn lookup_selected_entries_by_index<
        const N: usize,
        E: Entry + Indexed<N>,
        P: Fn(&E::Key) -> bool,
    >(
        &self,
        index: &E::Index,
        pred: P,
    ) -> Result<SelectedEntryIterator<Self::Db, E, P>, Error> {
        let index_bytes = E::index_to_bytes(index);

        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(SelectedEntryIterator::new(
                    self.prefix_iterator_cf(column_family, index_bytes),
                    pred,
                ))
            }
            None => Ok(SelectedEntryIterator::new(
                self.prefix_iterator(index_bytes),
                pred,
            )),
        }
    }

    fn iter_entries<E: Entry>(&self) -> Result<EntryIterator<Self::Db, E>, Error> {
        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(EntryIterator::new(
                    self.iterator_cf(column_family, IteratorMode::Start),
                ))
            }
            None => Ok(EntryIterator::new(self.iterator(IteratorMode::Start))),
        }
    }

    fn iter_selected_entries<E: Entry, P: Fn(&E::Key) -> bool>(
        &self,
        pred: P,
    ) -> Result<SelectedEntryIterator<Self::Db, E, P>, Error> {
        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(SelectedEntryIterator::new(
                    self.iterator_cf(column_family, IteratorMode::Start),
                    pred,
                ))
            }
            None => Ok(SelectedEntryIterator::new(
                self.iterator(IteratorMode::Start),
                pred,
            )),
        }
    }

    fn insert<E: Entry>(&self, key: &E::Key, value: &E::Value) -> Result<(), E::Error> {
        let key_bytes = E::key_to_bytes(key)?;
        let value_bytes = E::value_to_bytes(value)?;

        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                self.put_cf(column_family, key_bytes, value_bytes)
                    .map_err(E::Error::from)
            }
            None => self.put(key_bytes, value_bytes).map_err(E::Error::from),
        }
    }
}

impl Access for TransactionDB {
    type Db = Self;

    fn lookup_entry<E: Entry>(&self, key: &E::Key) -> Result<Option<E::Value>, E::Error> {
        let key_bytes = E::key_to_bytes(key)?;
        self.get_pinned(key_bytes)
            .map_err(E::Error::from)?
            .map_or(Ok(None), |value_bytes| {
                E::bytes_to_value(Cow::from(value_bytes.as_ref())).map(Some)
            })
    }

    fn lookup_entries<E: Entry, I: IntoIterator<Item = E::Key>>(
        &self,
        keys: I,
    ) -> Result<Vec<Option<E::Value>>, E::Error> {
        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| E::Error::from(Error::InvalidCfName(name.to_string())))?;

                let keys_bytes = keys
                    .into_iter()
                    .map(|key| E::key_to_bytes(&key).map(|bytes| (column_family, bytes)))
                    .collect::<Result<Vec<_>, _>>()?;

                self.multi_get_cf(keys_bytes)
                    .into_iter()
                    .map(|result| {
                        result
                            .map_err(E::Error::from)?
                            .map_or(Ok(None), |value_bytes| {
                                E::bytes_to_value(Cow::from(value_bytes)).map(Some)
                            })
                    })
                    .collect()
            }
            None => {
                let keys_bytes = keys
                    .into_iter()
                    .map(|key| E::key_to_bytes(&key))
                    .collect::<Result<Vec<_>, _>>()?;

                self.multi_get(keys_bytes)
                    .into_iter()
                    .map(|result| {
                        result
                            .map_err(E::Error::from)?
                            .map_or(Ok(None), |value_bytes| {
                                E::bytes_to_value(Cow::from(value_bytes)).map(Some)
                            })
                    })
                    .collect()
            }
        }
    }

    fn lookup_entries_by_index<const N: usize, E: Entry + Indexed<N>>(
        &self,
        index: &E::Index,
    ) -> Result<EntryIterator<Self::Db, E>, Error> {
        let index_bytes = E::index_to_bytes(index);

        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(EntryIterator::new(
                    self.prefix_iterator_cf(column_family, index_bytes),
                ))
            }
            None => Ok(EntryIterator::new(self.prefix_iterator(index_bytes))),
        }
    }

    fn lookup_selected_entries_by_index<
        const N: usize,
        E: Entry + Indexed<N>,
        P: Fn(&E::Key) -> bool,
    >(
        &self,
        index: &E::Index,
        pred: P,
    ) -> Result<SelectedEntryIterator<Self::Db, E, P>, Error> {
        let index_bytes = E::index_to_bytes(index);

        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(SelectedEntryIterator::new(
                    self.prefix_iterator_cf(column_family, index_bytes),
                    pred,
                ))
            }
            None => Ok(SelectedEntryIterator::new(
                self.prefix_iterator(index_bytes),
                pred,
            )),
        }
    }

    fn iter_entries<E: Entry>(&self) -> Result<EntryIterator<Self::Db, E>, Error> {
        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(EntryIterator::new(
                    self.iterator_cf(column_family, IteratorMode::Start),
                ))
            }
            None => Ok(EntryIterator::new(self.iterator(IteratorMode::Start))),
        }
    }

    fn iter_selected_entries<E: Entry, P: Fn(&E::Key) -> bool>(
        &self,
        pred: P,
    ) -> Result<SelectedEntryIterator<Self::Db, E, P>, Error> {
        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(SelectedEntryIterator::new(
                    self.iterator_cf(column_family, IteratorMode::Start),
                    pred,
                ))
            }
            None => Ok(SelectedEntryIterator::new(
                self.iterator(IteratorMode::Start),
                pred,
            )),
        }
    }

    fn insert<E: Entry>(&self, key: &E::Key, value: &E::Value) -> Result<(), E::Error> {
        let key_bytes = E::key_to_bytes(key)?;
        let value_bytes = E::value_to_bytes(value)?;

        match E::name() {
            Some(name) => {
                let column_family = self
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                self.put_cf(column_family, key_bytes, value_bytes)
                    .map_err(E::Error::from)
            }
            None => self.put(key_bytes, value_bytes).map_err(E::Error::from),
        }
    }
}

pub struct Transaction<'a> {
    pub underlying: rocksdb::Transaction<'a, TransactionDB>,
    db: Arc<TransactionDB>,
}

impl<'a> Transaction<'a> {
    pub fn new(
        underlying: rocksdb::Transaction<'a, TransactionDB>,
        db: Arc<TransactionDB>,
    ) -> Self {
        Self { underlying, db }
    }
}

impl<'a> Access for Transaction<'a> {
    type Db = rocksdb::Transaction<'a, TransactionDB>;

    fn lookup_entry<E: Entry>(&self, key: &E::Key) -> Result<Option<E::Value>, E::Error> {
        let key_bytes = E::key_to_bytes(key)?;
        self.underlying
            .get_pinned(key_bytes)
            .map_err(E::Error::from)?
            .map_or(Ok(None), |value_bytes| {
                E::bytes_to_value(Cow::from(value_bytes.as_ref())).map(Some)
            })
    }

    fn lookup_entries<E: Entry, I: IntoIterator<Item = E::Key>>(
        &self,
        keys: I,
    ) -> Result<Vec<Option<E::Value>>, E::Error> {
        match E::name() {
            Some(name) => {
                let column_family = self
                    .db
                    .cf_handle(name)
                    .ok_or_else(|| E::Error::from(Error::InvalidCfName(name.to_string())))?;

                let keys_bytes = keys
                    .into_iter()
                    .map(|key| E::key_to_bytes(&key).map(|bytes| (column_family, bytes)))
                    .collect::<Result<Vec<_>, _>>()?;

                self.underlying
                    .multi_get_cf(keys_bytes)
                    .into_iter()
                    .map(|result| {
                        result
                            .map_err(E::Error::from)?
                            .map_or(Ok(None), |value_bytes| {
                                E::bytes_to_value(Cow::from(value_bytes)).map(Some)
                            })
                    })
                    .collect()
            }
            None => {
                let keys_bytes = keys
                    .into_iter()
                    .map(|key| E::key_to_bytes(&key))
                    .collect::<Result<Vec<_>, _>>()?;

                self.underlying
                    .multi_get(keys_bytes)
                    .into_iter()
                    .map(|result| {
                        result
                            .map_err(E::Error::from)?
                            .map_or(Ok(None), |value_bytes| {
                                E::bytes_to_value(Cow::from(value_bytes)).map(Some)
                            })
                    })
                    .collect()
            }
        }
    }

    fn lookup_entries_by_index<const N: usize, E: Entry + Indexed<N>>(
        &self,
        index: &E::Index,
    ) -> Result<EntryIterator<Self::Db, E>, Error> {
        let index_bytes = E::index_to_bytes(index);

        match E::name() {
            Some(name) => {
                let column_family = self
                    .db
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(EntryIterator::new(
                    self.underlying
                        .prefix_iterator_cf(column_family, index_bytes),
                ))
            }
            None => Ok(EntryIterator::new(
                self.underlying.prefix_iterator(index_bytes),
            )),
        }
    }

    fn lookup_selected_entries_by_index<
        const N: usize,
        E: Entry + Indexed<N>,
        P: Fn(&E::Key) -> bool,
    >(
        &self,
        index: &E::Index,
        pred: P,
    ) -> Result<SelectedEntryIterator<Self::Db, E, P>, Error> {
        let index_bytes = E::index_to_bytes(index);

        match E::name() {
            Some(name) => {
                let column_family = self
                    .db
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(SelectedEntryIterator::new(
                    self.underlying
                        .prefix_iterator_cf(column_family, index_bytes),
                    pred,
                ))
            }
            None => Ok(SelectedEntryIterator::new(
                self.underlying.prefix_iterator(index_bytes),
                pred,
            )),
        }
    }

    fn iter_entries<E: Entry>(&self) -> Result<EntryIterator<Self::Db, E>, Error> {
        match E::name() {
            Some(name) => {
                let column_family = self
                    .db
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(EntryIterator::new(
                    self.underlying
                        .iterator_cf(column_family, IteratorMode::Start),
                ))
            }
            None => Ok(EntryIterator::new(
                self.underlying.iterator(IteratorMode::Start),
            )),
        }
    }

    fn iter_selected_entries<E: Entry, P: Fn(&E::Key) -> bool>(
        &self,
        pred: P,
    ) -> Result<SelectedEntryIterator<Self::Db, E, P>, Error> {
        match E::name() {
            Some(name) => {
                let column_family = self
                    .db
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                Ok(SelectedEntryIterator::new(
                    self.underlying
                        .iterator_cf(column_family, IteratorMode::Start),
                    pred,
                ))
            }
            None => Ok(SelectedEntryIterator::new(
                self.underlying.iterator(IteratorMode::Start),
                pred,
            )),
        }
    }

    fn insert<E: Entry>(&self, key: &E::Key, value: &E::Value) -> Result<(), E::Error> {
        let key_bytes = E::key_to_bytes(key)?;
        let value_bytes = E::value_to_bytes(value)?;

        match E::name() {
            Some(name) => {
                let column_family = self
                    .db
                    .cf_handle(name)
                    .ok_or_else(|| Error::InvalidCfName(name.to_string()))?;

                self.underlying
                    .put_cf(column_family, key_bytes, value_bytes)
                    .map_err(E::Error::from)
            }
            None => self
                .underlying
                .put(key_bytes, value_bytes)
                .map_err(E::Error::from),
        }
    }
}
