/// Indicates whether a database is opened in read-only or write mode at type level.
pub trait Mode: 'static {
    fn mode_type() -> ModeType;
}

/// Indicates whether a database is opened in read-only or write mode at value level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModeType {
    ReadOnly,
    Secondary,
    Writeable,
    Transactional
}

impl ModeType {
    pub fn is_primary(&self) -> bool {
        *self == Self::Writeable || *self == Self::Transactional
    }
}

#[derive(Clone, Copy)]
pub struct ReadOnly;

#[derive(Clone, Copy)]
pub struct Secondary;

#[derive(Clone, Copy)]
pub struct Writeable;

#[derive(Clone, Copy)]
pub struct Transactional;

impl Mode for ReadOnly {
    fn mode_type() -> ModeType {
        ModeType::ReadOnly
    }
}

impl Mode for Secondary {
    fn mode_type() -> ModeType {
        ModeType::Secondary
    }
}

impl Mode for Writeable {
    fn mode_type() -> ModeType {
        ModeType::Writeable
    }
}

impl Mode for Transactional {
    fn mode_type() -> ModeType {
        ModeType::Transactional
    }
}

/*
impl IsWriteable for Writeable {}
impl IsSecondary for Secondary {}
impl SinglePath for ReadOnly {}
impl SinglePath for Writeable {}

/// Indicates that a database is opened in write mode.
pub trait IsWriteable: Mode {}

/// Indicates that a database is opened in secondary mode and can try to catch up with the primary.
pub trait IsSecondary: Mode {}

/// Indicates that a database is opened in a mode that only requires a single path (i.e. not
/// secondary mode).
pub trait SinglePath: Mode {}
*/