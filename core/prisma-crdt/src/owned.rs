use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::SerializedField;

#[derive(Serialize, Deserialize, Clone)]
pub struct OwnedOperation {
	pub model: String,
	pub data: Vec<OwnedOperationData>,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum OwnedOperationData {
	#[serde(rename = "c")]
	Create(Map<String, Value>),
	#[serde(rename = "u")]
	Update(Map<String, Value>),
	#[serde(rename = "d")]
	Delete(Value),
}

pub trait Owned {
	type Field: Into<SerializedField>;
	type RequiredFields;

	const MODEL_NAME: &'static str;

	fn create_operation(
		id: i32,
		required_fields: Self::RequiredFields,
		fields: Vec<Self::Field>,
	) -> OwnedOperation;
	fn update_operation(id: i32, fields: Vec<Self::Field>) -> OwnedOperation;
	fn delete_operation(id: i32) -> OwnedOperation;
}
