//! Concrete implementations of the [`crate::ports`].
//!
//! * [`clock`] — system and test clocks.
//! * [`memory`] — dependency-free in-memory store (tests, examples, local
//!   dev).
//! * [`postgres`] — the production PostgreSQL adapter (feature `postgres`).

pub mod clock;
pub mod memory;

#[cfg(feature = "postgres")]
pub mod postgres;
