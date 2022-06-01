use serde_json::{Number, Value};
use sql_crdt::SerializedField;

use super::Id;

/// A system file path.
///
/// Is an example of owned data that is replicated to other nodes
/// using the owner node as the source of truth
pub struct FilePath {
	pub id: Id,
	pub path: String,
	// 1-M relation
	pub file: Option<Id>,
}

pub enum FilePathFields {
	Id(Id),
	Path(String),
	File(Option<Id>),
}

impl Into<SerializedField> for FilePathFields {
	fn into(self) -> SerializedField {
		match self {
			FilePathFields::Id(id) => ("id".to_string(), Value::Number(Number::from(id))),
			FilePathFields::Path(path) => ("path".to_string(), Value::String(path)),
			FilePathFields::File(file) => (
				"file".to_string(),
				file.map(|file| Value::Number(Number::from(file)))
					.unwrap_or(Value::Null),
			),
		}
	}
}

// macro/codegen
// impl SharedRecord for FilePath {
// 	type Fields = FilePathFields;
// }
