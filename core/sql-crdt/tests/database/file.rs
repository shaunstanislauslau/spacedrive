use super::Id;

/// A unique file that can exist in multiple locations
///
/// Existence of this record is derived from the existence of the file,
/// but properties can be modified like a Shared Record
pub struct File {
	pub id: Id,
	pub name: String,
}

pub enum FileField {
	Name(String),
}
