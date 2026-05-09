mod manager;
mod persistence;
mod state;

// Preserve the historical `crate::index::IndexIoOperation` path even though
// current code only constructs it from inside `manager`.
#[allow(unused_imports)]
pub use self::manager::IndexIoOperation;
// Preserve the historical `crate::index::IntentGuard` path for internal users.
#[allow(unused_imports)]
pub(crate) use self::manager::IntentGuard;
pub(crate) use self::manager::{Index, IntentMeta};
pub use self::manager::{IndexError, IndexReadGuard};
pub use self::state::IndexStateItem;
