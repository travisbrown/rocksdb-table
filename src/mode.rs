pub trait Mode: 'static {
    fn is_read_only() -> bool;
}

/// Indicates that a database is opened in write mode.
pub trait IsWriteable: Mode {}

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
