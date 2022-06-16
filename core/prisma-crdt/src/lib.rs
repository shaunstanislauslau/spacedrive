pub mod generator;
pub mod local;
pub mod owned;
pub mod relation;
pub mod shared;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uhlc::NTP64;

pub use owned::*;
pub use relation::*;
pub use shared::*;

pub type Id = Vec<u8>;

/// An operation on a CRDT - either a shared record or a many relation.
/// All CRDT operations record the `node` and `timestamp` the associated with them.
///
/// The state of a CRDT that an operation acts on is just the result of all previous operations,
/// so `CRDTOperation` is designed to be sent via any transport to any node that can resolve
/// that state.
#[derive(Serialize, Deserialize, Clone)]
pub struct CRDTOperation {
	pub node: Id,
	pub timestamp: NTP64, // HLC
	#[serde(flatten)]
	pub typ: CRDTOperationType,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum CRDTOperationType {
	Shared(SharedOperation),
	Relation(RelationOperation),
	Owned(OwnedOperation),
}

pub struct CRDTStore<Database> {
	pub database: Database,
}

pub type SerializedField = (String, Value);
