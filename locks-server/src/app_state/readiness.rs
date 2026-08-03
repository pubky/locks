/// Runtime composition state for the Lock Server process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeStorageKind {
    InMemory,
    Postgres,
}
