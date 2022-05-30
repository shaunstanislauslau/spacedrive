use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// An operation on a shared record CRDT.
/// Shared records are identified by their `model` (db table) and `id` (uuid).
///
/// ## Create
/// Creating a shared record simply requires providing its properties.
/// The record is created with the type of `model` and the provided `id`,
/// along with the provided data.
///
/// ## Update
/// Updates to shared records must be done on a per-field basis,
/// ie. multiple fields cannot be updated in a single operation.
/// If multiple updates were permitted in one operation, determining the most
/// recent update for a field would be significantly more difficult,
/// since each operation would have to be searched for what fields they affect.
/// Sure, it could be done, but requiring one operation per update is more simple.
///
/// ## Delete
/// Deleting a shared record uses the operation's `record_id` and `model` to identify the record and delete.
#[derive(Serialize, Deserialize)]
pub struct SharedRecordOperation {
	pub record_id: Uuid,
	pub model: String,
	#[serde(flatten)]
	pub data: SharedRecordOperationData,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SharedRecordOperationData {
	Create { data: Value },
	Update { field: String, value: Value },
	Delete,
}
