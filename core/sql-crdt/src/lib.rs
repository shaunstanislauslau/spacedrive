pub mod many_relation;
pub mod shared_record;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use many_relation::*;
pub use shared_record::*;
use uhlc::Timestamp;

pub type Id = u32;

/// An operation on a CRDT - either a shared record or a many relation.
/// All CRDT operations record the `node` and `timestamp` the associated with them.
///
/// The state of a CRDT that an operation acts on is just the result of all previous operations,
/// so `CRDTOperation` is designed to be sent via any transport to any node that can resolve
/// that state.
#[derive(Serialize, Deserialize, Clone)]
pub struct CRDTOperation {
	pub node: Id,
	pub timestamp: Timestamp, // HLC
	#[serde(flatten)]
	pub typ: CRDTOperationType,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum CRDTOperationType {
	SharedRecord(SharedRecordOperation),
	ManyRelation(ManyRelationOperation),
}

pub struct CRDTStore<Database> {
	pub database: Database,
}

pub type SerializedField = (String, Value);
