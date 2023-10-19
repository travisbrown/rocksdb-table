use rocksdb::{DBWithThreadMode, SingleThreaded, TransactionDB, TransactionDBOptions, DB};
use std::{marker::PhantomData, path::Path, sync::Arc};

use crate::{
    access::Access,
    entry::{Entry, Indexed},
    error::Error,
    iter::{EntryIterator, SelectedEntryIterator},
    mode::{IsWriteable, Mode, Writeable},
    DatabaseOptions,
};

/// A wrapper for a RocksDB database that maintains type information about whether it was opened
/// in read-only mode.
#[derive(Clone)]
pub struct Database<D, M> {
    pub db: Arc<D>,
    pub options: DatabaseOptions,
    _mode: PhantomData<M>,
}

impl<D: Access, M: Mode> Database<D, M> {
    pub fn lookup<E: Entry>(&self, key: &E::Key) -> Result<Option<E::Value>, E::Error> {
        D::lookup_entry::<E>(&self.db, key)
    }

    pub fn multi_lookup<E: Entry, I: IntoIterator<Item = E::Key>>(
        &self,
        keys: I,
    ) -> Result<Vec<Option<E::Value>>, E::Error> {
        D::lookup_entries::<E, _>(&self.db, keys)
    }

    pub fn lookup_index<const N: usize, E: Entry + Indexed<N>>(
        &self,
        index: &E::Index,
    ) -> Result<EntryIterator<D::Db, E>, Error> {
        D::lookup_entries_by_index(&self.db, index)
    }

    pub fn lookup_index_selected<const N: usize, E: Entry + Indexed<N>, P: Fn(&E::Key) -> bool>(
        &self,
        index: &E::Index,
        pred: P,
    ) -> Result<SelectedEntryIterator<D::Db, E, P>, Error> {
        D::lookup_selected_entries_by_index(&self.db, index, pred)
    }

    pub fn iter<E: Entry>(&self) -> Result<EntryIterator<D::Db, E>, Error> {
        D::iter_entries(&self.db)
    }

    pub fn iter_selected<E: Entry, P: Fn(&E::Key) -> bool>(
        &self,
        pred: P,
    ) -> Result<SelectedEntryIterator<D::Db, E, P>, Error> {
        D::iter_selected_entries(&self.db, pred)
    }
}

impl<D: Access, M: IsWriteable> Database<D, M> {
    pub fn insert<E: Entry>(&self, key: &E::Key, value: &E::Value) -> Result<(), E::Error> {
        D::insert::<E>(&self.db, key, value)
    }
}

impl<M: Mode> Database<DBWithThreadMode<SingleThreaded>, M> {
    pub fn open<P: AsRef<Path>>(
        path: P,
        options: DatabaseOptions,
    ) -> Result<Self, crate::error::Error> {
        let column_families = options.column_families();

        let db = if M::is_read_only() {
            if column_families.is_empty() {
                DB::open_for_read_only(&options.options, path, true)?
            } else {
                DB::open_cf_descriptors_read_only(&options.options, path, column_families, true)?
            }
        } else if column_families.is_empty() {
            DB::open(&options.options, path)?
        } else {
            DB::open_cf_descriptors(&options.options, path, column_families)?
        };

        Ok(Self {
            db: Arc::new(db),
            options,
            _mode: PhantomData,
        })
    }
}

impl Database<TransactionDB, Writeable> {
    pub fn open_transactional<P: AsRef<Path>>(
        path: P,
        options: DatabaseOptions,
        transaction_options: TransactionDBOptions,
    ) -> Result<Self, crate::error::Error> {
        let column_families = options.column_families();

        let db = if column_families.is_empty() {
            TransactionDB::open(&options.options, &transaction_options, path)?
        } else {
            TransactionDB::open_cf_descriptors(
                &options.options,
                &transaction_options,
                path,
                column_families,
            )?
        };

        Ok(Self {
            db: Arc::new(db),
            options,
            _mode: PhantomData,
        })
    }

    pub fn transaction(&self) -> crate::access::Transaction {
        crate::access::Transaction::new(self.db.transaction(), self.db.clone())
    }
}
