/// Clock boundary for deterministic time-sensitive use cases.
pub trait Clock: Send + Sync {
    /// Returns the current UTC timestamp.
    fn now(&self) -> time::OffsetDateTime;
}
