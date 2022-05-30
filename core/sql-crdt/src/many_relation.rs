use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
#[derive(Serialize, Deserialize)]
pub struct ManyRelationOperation {
	pub relation_item: Uuid,
	pub relation_group: Uuid,
	pub relation: String,
	#[serde(flatten)]
	pub data: ManyRelationOperationData,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ManyRelationOperationData {
	Create,
	Update { field: String, value: String },
	Delete,
}
