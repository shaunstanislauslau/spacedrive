use serde_json::{Number, Value};
use sql_crdt::{SerializedField, SharedRecord, SharedRecordOperation};

use super::Id;

/// A tag that can be applied to many files on any node.
///
/// Is an example of a Shared Record, as any of its properties
/// can be modified by any node
pub struct Tag {
	pub id: Id,
	pub name: String,
	pub color: String,
}

#[derive(Clone)]
pub enum TagField {
	Id(Id),
	Name(String),
	Color(String),
}

impl Into<SerializedField> for TagField {
	fn into(self) -> SerializedField {
		match self {
			TagField::Id(id) => ("id".to_string(), Value::Number(Number::from(id))),
			TagField::Name(name) => ("name".to_string(), Value::String(name)),
			TagField::Color(color) => ("color".to_string(), Value::String(color)),
		}
	}
}

impl TryFrom<SerializedField> for TagField {
	type Error = ();

	fn try_from((key, value): SerializedField) -> Result<Self, Self::Error> {
		match (key.as_str(), value) {
			("id", Value::Number(value)) => Ok(TagField::Id(value.as_u64().unwrap() as u32)),
			("name", Value::String(value)) => Ok(TagField::Name(value)),
			("color", Value::String(value)) => Ok(TagField::Color(value)),
			_ => Err(()),
		}
	}
}

impl SharedRecord for Tag {
	type Field = TagField;
	type RequiredFields = (Id, String, String);
	const MODEL_NAME: &'static str = "Tag";

	fn create_operation(
		id: Id,
		required_fields: Self::RequiredFields,
		fields: Vec<Self::Field>,
	) -> SharedRecordOperation {
		let mut required_fields_vec = {
			let (id, name, color) = required_fields;
			vec![
				TagField::Id(id),
				TagField::Name(name),
				TagField::Color(color),
			]
		};

		required_fields_vec.extend(fields);

		let map = required_fields_vec
			.into_iter()
			.map(Into::<SerializedField>::into)
			.collect();

		SharedRecordOperation::new_create(id, Self::MODEL_NAME, map)
	}

	fn update_operation(id: Id, field: Self::Field) -> SharedRecordOperation {
		let (field, value): SerializedField = field.into();

		SharedRecordOperation::new_update(id, Self::MODEL_NAME, field, value)
	}

	fn delete_operation(id: Id) -> SharedRecordOperation {
		SharedRecordOperation::new_delete(id, Self::MODEL_NAME)
	}
}
