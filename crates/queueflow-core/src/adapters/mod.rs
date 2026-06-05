//! Concrete implementations of the [`crate::ports`].
//!
//! * [`clock`] — system and test clocks.
//! * [`memory`] — dependency-free in-memory store + queue (tests, examples,
//!   local dev).
//! * [`postgres`] — production PostgreSQL + PGMQ adapters (feature `postgres`).

pub mod clock;
pub mod memory;

#[cfg(feature = "postgres")]
pub mod postgres;
