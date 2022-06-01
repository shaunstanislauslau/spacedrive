use std::ops::Deref;

use sql_crdt::{
	CRDTOperation, CRDTOperationType, Id, SharedRecord, SharedRecordOperation,
	SharedRecordOperationData,
};
use uhlc::HLC;

use self::tag_on_file::TagOnFile;

mod file;
mod file_path;
mod operation;
mod tag;
mod tag_on_file;

pub use file::*;
pub use file_path::*;
pub use operation::*;
pub use tag::*;
pub use tag_on_file::*;

/// A mock database to build on top of.
///
/// This should not be used directly in tests, as the level of access
/// it provides to underlying data is too permissive for how sql-crdt allows.
pub struct Database {
	file_paths: Vec<FilePath>,
	files: Vec<File>,
	tags: Vec<Tag>,
	tags_on_files: Vec<TagOnFile>,
	shared_record_operations: Vec<operation::SharedRecordOperation>,
	_id: u32,
}

impl Database {
	pub fn new() -> Database {
		Database {
			file_paths: vec![],
			files: vec![],
			tags: vec![],
			tags_on_files: vec![],
			shared_record_operations: vec![],
			_id: 0,
		}
	}

	fn generate_id(&mut self) -> u32 {
		self._id += 1;
		self._id
	}

	pub fn file_path(&self, id: Id) -> Option<&FilePath> {
		self.file_paths.iter().find(|file_path| file_path.id == id)
	}
	pub fn file(&self, id: Id) -> Option<&File> {
		self.files.iter().find(|file| file.id == id)
	}
	pub fn files_with_tag(&self, tag_id: Id) -> Vec<&File> {
		self.files
			.iter()
			.filter(|file| {
				self.tags_on_files
					.iter()
					.any(|tag_on_file| tag_on_file.file == file.id && tag_on_file.tag == tag_id)
			})
			.collect()
	}
	pub fn tag(&self, id: Id) -> Option<&Tag> {
		self.tags.iter().find(|tag| tag.id == id)
	}
	pub fn tags_on_file(&self, file_id: Id) -> Vec<&Tag> {
		self.tags
			.iter()
			.filter(|tag| {
				self.tags_on_files
					.iter()
					.any(|tag_on_file| tag_on_file.file == file_id && tag_on_file.tag == tag.id)
			})
			.collect()
	}

	pub fn add_file_path(&mut self, id: Id, path: &str, file: Option<Id>) -> &FilePath {
		self.file_paths.push(FilePath {
			id,
			path: path.to_string(),
			file,
		});

		self.file_paths.last().unwrap()
	}
	pub fn add_file(&mut self, id: Id, name: &str, paths: Vec<Id>, tags: Vec<Id>) -> &File {
		self.files.push(File {
			id,
			name: name.to_string(),
		});

		for path_id in paths {
			self.update_file_path(path_id, vec![FilePathFields::File(Some(id))]);
		}

		for tag in tags {
			self.add_tag_on_file(tag, id);
		}

		self.files.last().unwrap()
	}
	pub fn add_tag(&mut self, id: Id, name: String, color: String, files: Vec<Id>) -> &Tag {
		self.tags.push(Tag { id, name, color });

		for file in files {
			self.add_tag_on_file(id, file);
		}

		self.tags.last().unwrap()
	}
	pub fn add_tag_on_file(&mut self, tag: Id, file: Id) -> &TagOnFile {
		self.tags_on_files.push(TagOnFile { tag, file });

		self.tags_on_files.last().unwrap()
	}

	pub fn update_file_path(&mut self, id: Id, updates: Vec<FilePathFields>) {
		if let Some(file_path) = self.file_paths.iter_mut().find(|fp| fp.id == id) {
			for update in updates {
				match update {
					FilePathFields::Id(id) => file_path.id = id,
					FilePathFields::Path(path) => file_path.path = path,
					FilePathFields::File(file) => file_path.file = file,
				}
			}
		}
	}
	pub fn update_file(&mut self, id: Id, updates: Vec<FileField>) {
		if let Some(file) = self.files.iter_mut().find(|file| file.id == id) {
			for update in updates {
				match update {
					FileField::Name(name) => file.name = name,
				}
			}
		}
	}
	pub fn update_tag(&mut self, id: Id, updates: Vec<TagField>) {
		if let Some(tag) = self.tags.iter_mut().find(|tag| tag.id == id) {
			for update in updates {
				match update {
					TagField::Id(id) => tag.id = id,
					TagField::Name(name) => tag.name = name,
					TagField::Color(color) => tag.color = color,
				}
			}
		}
	}

	pub fn remove_file_path(&mut self, id: Id) {
		self.file_paths.retain(|file_path| file_path.id != id);

		let file_path = match self.file_path(id) {
			None => return,
			Some(file_path) => file_path,
		};

		if let Some(file_id) = file_path.file {
			if let None = self
				.file_paths
				.iter()
				.find(|file_path| file_path.file == Some(file_id))
			{
				self.remove_file(file_id);
			}
		}
	}
	pub fn remove_file(&mut self, id: Id) {
		self.files.retain(|file| file.id != id);

		self.file_paths
			.iter_mut()
			.filter(|file_path| file_path.file == Some(id))
			.for_each(|file_path| file_path.file = None);

		self.tags_on_files
			.retain(|tag_on_file| tag_on_file.file != id);
	}
	pub fn remove_tag(&mut self, id: Id) {
		self.tags.retain(|tag| tag.id != id);

		let tag = match self.tag(id) {
			None => return,
			Some(tag) => tag,
		};

		self.tags_on_files
			.retain(|tag_on_file| tag_on_file.tag != id);
	}
	pub fn remove_tag_on_file(&mut self, tag: Id, file: Id) {
		self.tags_on_files
			.retain(|tag_on_file| tag_on_file.tag != tag || tag_on_file.file != file);
	}
}

pub struct CRDTDatabase {
	node_id: Id,
	database: Database,
	clock: HLC,
}

impl Deref for CRDTDatabase {
	type Target = Database;

	fn deref(&self) -> &Self::Target {
		&self.database
	}
}

impl CRDTDatabase {
	pub fn new(node_id: Id, clock: HLC) -> CRDTDatabase {
		CRDTDatabase {
			node_id,
			clock,
			database: Database::new(),
		}
	}

	pub fn database(&self) -> &Database {
		&self.database
	}

	fn new_operation(&mut self, typ: CRDTOperationType) -> CRDTOperation {
		let op = CRDTOperation {
			node: self.node_id,
			timestamp: self.clock.new_timestamp(),
			typ,
		};

		self.store_operation(&op);

		op
	}

	pub fn create_tag(&mut self, id: Id, name: String, color: String) -> CRDTOperation {
		self.database
			.add_tag(id, name.clone(), color.clone(), vec![]);

		let tag_op = Tag::create_operation(id, (id, name, color), vec![]);

		let op = self.new_operation(CRDTOperationType::SharedRecord(tag_op));

		op
	}

	pub fn update_tag(&mut self, id: Id, updates: Vec<TagField>) -> Vec<CRDTOperation> {
		self.database.update_tag(id, updates.clone());

		updates
			.into_iter()
			.map(|update| {
				let tag_op = Tag::update_operation(id, update);

				let op = self.new_operation(CRDTOperationType::SharedRecord(tag_op));

				op
			})
			.collect()
	}

	pub fn delete_tag(&mut self, id: Id) -> CRDTOperation {
		self.database.remove_tag(id);

		let tag_op = Tag::delete_operation(id);

		let op = self.new_operation(CRDTOperationType::SharedRecord(tag_op));

		op
	}

	pub fn store_operation(&mut self, op: &CRDTOperation) {
		let CRDTOperation {
			node,
			timestamp,
			typ,
		} = &op;

		match typ {
			CRDTOperationType::SharedRecord(tag_op) => {
				let SharedRecordOperation {
					record_id,
					model,
					data,
				} = tag_op;

				self.database
					.shared_record_operations
					.push(operation::SharedRecordOperation {
						node: *node,
						timestamp: timestamp.clone(),
						record_id: *record_id,
						model: model.to_string(),
						typ: match data {
							SharedRecordOperationData::Create { .. } => "c".to_string(),
							SharedRecordOperationData::Update { field, .. } => {
								"u".to_string() + field
							}
							SharedRecordOperationData::Delete => "d".to_string(),
						},
						data: data.clone(),
					})
			}
			_ => todo!(),
		}
	}

	pub fn apply_operation(&mut self, op: CRDTOperation) {
		let CRDTOperation {
			node,
			timestamp,
			typ,
		} = op;

		match typ {
			CRDTOperationType::SharedRecord(op) => {
				let SharedRecordOperation {
					record_id,
					model,
					data,
				} = op;

				// Conflict resolution
				match &data {
					// if update, check for newer updates to field.
					SharedRecordOperationData::Update { field, .. } => {
						for op in &self.database.shared_record_operations {
							if op.record_id == record_id
								&& op.typ == "u".to_string() + field
								&& op.timestamp > timestamp
							{
								// if newer update exists, ignore
								return;
							}
							// if not, accept
						}
					}
					// if create or delete, always accept
					SharedRecordOperationData::Create { .. }
					| SharedRecordOperationData::Delete => {}
				}

				// Apply change to database
				match model.as_str() {
					Tag::MODEL_NAME => match data {
						SharedRecordOperationData::Create { data } => {
							let mut fields: Vec<TagField> = data
								.into_iter()
								.map(|serialized_field| serialized_field.try_into().unwrap())
								.collect();

							let id = fields
								.iter()
								.position(|field| matches!(field, TagField::Id(_)))
								.map(|i| fields.swap_remove(i));
							let name = fields
								.iter()
								.position(|field| matches!(field, TagField::Name(_)))
								.map(|i| fields.swap_remove(i));
							let color = fields
								.iter()
								.position(|field| matches!(field, TagField::Color(_)))
								.map(|i| fields.swap_remove(i));

							let (id, name, color) = match (id, name, color) {
								(
									Some(TagField::Id(id)),
									Some(TagField::Name(name)),
									Some(TagField::Color(color)),
								) => (id, name, color),
								_ => panic!("Invalid tag create operation"),
							};

							self.database.add_tag(id, name, color, vec![]);
						}
						SharedRecordOperationData::Update { field, value } => {
							let field: TagField = (field, value).try_into().unwrap();

							self.database.update_tag(record_id, vec![field]);
						}
						SharedRecordOperationData::Delete => {
							self.database.remove_tag(record_id);
						}
					},
					_ => todo!(),
				}
			}
			_ => todo!(),
		}
	}
}
