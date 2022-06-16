use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{Id, SerializedField};

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
#[derive(Serialize, Deserialize, Clone)]
pub struct SharedOperation {
	pub record_id: Id, // Uuid,
	pub model: String,
	#[serde(flatten)]
	pub data: SharedOperationData,
}

impl SharedOperation {
	fn new(record_id: Id, model: String, data: SharedOperationData) -> Self {
		Self {
			record_id,
			model,
			data,
		}
	}

	pub fn new_create(record_id: Id, model: &str, data: Map<String, Value>) -> Self {
		SharedOperation::new(
			record_id,
			model.to_string(),
			SharedOperationData::Create { data },
		)
	}

	pub fn new_update(record_id: Id, model: &str, field: String, value: Value) -> Self {
		SharedOperation::new(
			record_id,
			model.to_string(),
			SharedOperationData::Update { field, value },
		)
	}

	pub fn new_delete(record_id: Id, model: &str) -> Self {
		SharedOperation::new(
			record_id,
			model.to_string(),
			SharedOperationData::Delete,
		)
	}
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum SharedOperationData {
	Create { data: Map<String, Value> },
	Update { field: String, value: Value },
	Delete,
}

// TODO: Use UUID
pub trait SharedRecord {
	type Field: Into<SerializedField>;
	type RequiredFields;

	const MODEL_NAME: &'static str;

	fn create_operation(
		id: Id,
		required_fields: Self::RequiredFields,
		fields: Vec<Self::Field>,
	) -> SharedOperation;
	fn update_operation(id: Id, field: Self::Field) -> SharedOperation;
	fn delete_operation(id: Id) -> SharedOperation;
}
