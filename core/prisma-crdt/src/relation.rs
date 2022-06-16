use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Id, SerializedField};

/// An operation on a many relation CRDT.
/// Many relations are identified by their `relation` (db table),
/// `relation_item` (subject record) and `relation_group` (group record).
///
/// Many relations represent a Many to Many (M2M) relation between two records,
/// where data about the relation itself is stored in a separate table.
///
/// **NOTE**: This does not include M2M relations where the item can exist in a group multiple times.
///
/// In contrast to shared records, many relations are identified by the records they relate,
/// and do not have their own unique ID.
///
/// ## Create
/// Creating a many relation does not allow for setting data, only for indicating that the relation exists.
/// Setting data can be done with subsequent Update operations. This is enforced as if multiple nodes attempt
/// to create the same relation, multiple relations should not be created - hence the lack of unique IDs for many relations.
///
/// ## Update
/// Updates to many relations are done on a per-field basis, in the same way as shared records.
///
/// ## Delete
/// Deleting many relations use the operation's `relation`, `relation_item` and `relation_group` to identify the relation and delete it.
#[derive(Serialize, Deserialize, Clone)]
pub struct RelationOperation {
	pub relation_item: Id,
	pub relation_group: Id,
	pub relation: String,
	#[serde(flatten)]
	pub data: RelationRecordOperationData,
}

impl RelationOperation {
	fn new(
		relation_item: Id,
		relation_group: Id,
		relation: String,
		data: RelationRecordOperationData,
	) -> Self {
		Self {
			relation_item,
			relation_group,
			relation,
			data,
		}
	}

	pub fn new_create(relation_item: Id, relation_group: Id, relation: &str) -> Self {
		Self::new(
			relation_item,
			relation_group,
			relation.to_string(),
			RelationRecordOperationData::Create,
		)
	}

	pub fn new_update(
		relation_item: Id,
		relation_group: Id,
		relation: &str,
		field: String,
		value: Value,
	) -> Self {
		Self::new(
			relation_item,
			relation_group,
			relation.to_string(),
			RelationRecordOperationData::Update { field, value },
		)
	}

	pub fn new_delete(relation_item: Id, relation_group: Id, relation: &str) -> Self {
		Self::new(
			relation_item,
			relation_group,
			relation.to_string(),
			RelationRecordOperationData::Delete,
		)
	}
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum RelationRecordOperationData {
	Create,
	Update { field: String, value: Value },
	Delete,
}

pub trait RelationRecord {
	type Field: Into<SerializedField>;

	const RELATION_NAME: &'static str;
	
	fn create_operation(relation_item: Id, relation_group: Id) -> RelationOperation;
	fn update_operation(
		relation_item: Id,
		relation_group: Id,
		field: Self::Field,
	) -> RelationOperation;
	fn delete_operation(relation_item: Id, relation_group: Id) -> RelationOperation;
}
