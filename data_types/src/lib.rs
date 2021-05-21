//! This crate contains the data types that are shared between InfluxDB IOx
//! servers including replicated data, rules for how data is split up and
//! queried, and what gets stored in the write buffer database.

#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod chunk_metadata;
pub mod consistent_hasher;
mod database_name;
pub use database_name::*;
pub mod database_rules;
pub mod error;
pub mod job;
pub mod names;
pub mod partition_metadata;
pub mod server_id;
pub mod timestamp;
