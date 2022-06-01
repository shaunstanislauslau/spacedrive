use super::Id;

/// A record representing a tag being assigned to a file.
/// Exists as a separate record as it is a M-M relation.
///
/// Is an example of a Many Relation, most apparent by it's 2 foreign Ids
/// and no unique id
pub struct TagOnFile {
	pub tag: Id,
	pub file: Id,
}