pub mod many_relation;
pub mod shared_record;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use many_relation::ManyRelationOperation;
use shared_record::SharedRecordOperation;

/// An operation on a CRDT - either a shared record or a many relation.
/// All CRDT operations record the `node` and `timestamp` the associated with them.
///
/// The state of a CRDT that an operation acts on is just the result of all previous operations,
/// so `CRDTOperation` is designed to be sent via any transport to any node that can resolve
/// that state.
#[derive(Serialize, Deserialize)]
pub struct CRDTOperation {
	pub node: Uuid,
	pub timestamp: String, // HLC
	#[serde(flatten)]
	pub typ: CRDTOperationType,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CRDTOperationType {
	SharedRecord(SharedRecordOperation),
	ManyRelation(ManyRelationOperation),
}
