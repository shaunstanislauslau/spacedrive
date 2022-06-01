use sql_crdt::{Id, SharedRecordOperationData};
use uhlc::Timestamp;

pub struct SharedRecordOperation {
	pub node: Id,
	pub timestamp: Timestamp,
	pub record_id: Id,
	pub model: String,
	// type + field if update
	pub typ: String,
	pub data: SharedRecordOperationData,
}
