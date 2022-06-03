use std::collections::HashMap;
use std::{fs, io};
use std::path::Path;
use crate::job::JobReportUpdate;
use crate::prisma::file;
use crate::sys::get_location;
use crate::{
	file::FileError,
	job::{Job, WorkerContext},
	prisma::file_path,
	CoreContext,
};
use futures::executor::block_on;
use prisma_client_rust::prisma_models::PrismaValue;
use prisma_client_rust::raw::Raw;
use prisma_client_rust::{raw, Direction};
use serde::{Deserialize, Serialize};
use super::checksum::generate_cas_id;

#[derive(Deserialize, Serialize, Debug)]
pub struct FileCreated {
	pub id: i32,
	pub cas_id: String,
}

#[derive(Debug)]
pub struct FileIdentifierJob {
	pub location_id: i32,
	pub path: String,
}

// The identifier job generates cas ids for files and creates unique file records.
// Since the indexer has already taken care of inserting orphan file paths, this job can fail and pick up where it left off.
#[async_trait::async_trait]
impl Job for FileIdentifierJob {
	fn name(&self) -> &'static str {
		"file_identifier"
	}
	async fn run(&self, ctx: WorkerContext) -> Result<(), Box<dyn std::error::Error>> {
		println!("Identifying files");
		let location = get_location(&ctx.core_ctx, self.location_id).await?;
		let location_path = location.path.unwrap_or("".to_string());

		let total_count = count_orphan_file_paths(&ctx.core_ctx, location.id.into()).await?;
		println!("Found {} orphan file paths", total_count);
		
		// Chunk the file paths into batches of 100
		let task_count = (total_count as f64 / 100f64).ceil() as usize;
		println!("Will process {} tasks", task_count);

		// update job with total task count based on orphan file_paths count
		ctx.progress(vec![JobReportUpdate::TaskCount(task_count)]);

		let db = ctx.core_ctx.database.clone();

		let _ctx = tokio::task::spawn_blocking(move || {
			let mut completed: usize = 0;
			let mut cursor: i32 = 1;
			// map cas_id to file_path ids
			let mut cas_id_lookup: HashMap<String, (i32, i32)> = HashMap::new();

			while completed < task_count {
				// get the orphan file paths
				let file_paths = block_on(get_orphan_file_paths(&ctx.core_ctx, cursor)).unwrap();
				println!(
					"Processing {:?} orphan files. ({} completed of {})",
					file_paths.len(),
					completed,
					task_count
				);

				// get the next file id
				#[derive(Deserialize, Serialize, Debug)]
				struct QueryRes {
					id: Option<i32>,
				}
				let mut next_file_id = match block_on(db
				._query_raw::<QueryRes>(raw!("SELECT MAX(id) id FROM files")))
				{
					Ok(rows) => rows[0].id.unwrap_or(0),
					Err(e) => panic!("Error querying for next file id: {}", e),
				};

				// raw values to be inserted into the database
				let mut values: Vec<PrismaValue> = Vec::new();

				// prepare unique file values for each file path
				for file_path in file_paths.iter() {
					let file_id = next_file_id.clone();
					println!("Next file id: {}", file_id);
					// get the values for this unique file
					match prepare_file_values(&file_id, &location_path, file_path) {
						Ok((cas_id, data)) => {
							// add unique file id to cas_id lookup map
							cas_id_lookup.insert(cas_id, (file_path.id, file_id));
							// add values to raw query data
							values.extend(data);
							next_file_id += 1;
						}
						Err(e) => {
							println!("Error processing file: {}", e);
							continue;
						}
					};
				}
				if values.len() == 0 {
					println!("No orphan files to process, finishing...");
					break;
				}

				println!("Inserting {} unique file records ({:?} values)", file_paths.len(), values.len());
				// insert files
				let files: Vec<FileCreated> = block_on(db._query_raw(Raw::new(
				  &format!(
				    "INSERT INTO files (id, cas_id, size_in_bytes) VALUES {} ON CONFLICT (cas_id) DO NOTHING RETURNING id, cas_id",
				    vec!["({}, {}, {})"; file_paths.len()].join(",")
				  ),
				  values
				))).unwrap_or_else(|e| {
					println!("Error inserting files: {}", e);
					Vec::new()
				});
				
				println!("Assigning {} unique file ids to origin file_paths", files.len());
				// assign unique file to file path
				for (_cas_id, (file_path_id, file_id)) in cas_id_lookup.iter() {
				  block_on(
				    db.file_path()
				      .find_unique(file_path::id::equals(file_path_id.clone()))
				      .update(vec![
				        file_path::file_id::set(Some(file_id.clone()))
				      ])
				      .exec()
				  ).unwrap();
				}
				// handle cursor
				let last_row = file_paths.last().unwrap();
				cursor = last_row.id;
				completed += 1;
				// update progress
				ctx.progress(vec![
				  JobReportUpdate::CompletedTaskCount(completed),
				  JobReportUpdate::Message(format!(
				    "Processed {} of {} orphan files",
				    completed,
				    task_count
				  )),
				]);
			}
			ctx
		})
		.await?;

		// let remaining = count_orphan_file_paths(&ctx.core_ctx, location.id.into()).await?;
		Ok(())
	}
}

#[derive(Deserialize, Serialize, Debug)]
struct CountRes {
	count: Option<usize>,
}

pub async fn count_orphan_file_paths(
	ctx: &CoreContext,
	location_id: i64,
) -> Result<usize, FileError> {
	let db = &ctx.database;
	let files_count = db
		._query_raw::<CountRes>(raw!(
			"SELECT COUNT(*) AS count FROM file_paths WHERE file_id IS NULL AND is_dir IS FALSE AND location_id = {}",
			PrismaValue::Int(location_id)
		))
		.await?;
	Ok(files_count[0].count.unwrap_or(0))
}

pub async fn get_orphan_file_paths(
	ctx: &CoreContext,
	cursor: i32,
) -> Result<Vec<file_path::Data>, FileError> {
	let db = &ctx.database;
	println!("cursor: {:?}", cursor);
	let files = db
		.file_path()
		.find_many(vec![
			file_path::file_id::equals(None),
			file_path::is_dir::equals(false),
		])
		.order_by(file_path::id::order(Direction::Asc))
		.cursor(file_path::id::cursor(cursor))
		.take(100)
		.exec()
		.await?;
	Ok(files)
}

pub fn prepare_file_values(
	id: &i32,
	location_path: &str,
	file_path: &file_path::Data,
) -> Result<(String, [PrismaValue; 3]), io::Error> {
	let path = Path::new(&location_path).join(Path::new(file_path.materialized_path.as_str()));
	// println!("Processing file: {:?}", path);
	let metadata = fs::metadata(&path)?;
	let size = metadata.len();
	let cas_id = {
		if !file_path.is_dir {
			let mut ret = generate_cas_id(path.clone(), size.clone()).unwrap();
			ret.truncate(20);
			ret
		} else {
			"".to_string()
		}
	};

	Ok((cas_id.clone(), [PrismaValue::Int(id.clone().into()), PrismaValue::String(cas_id), PrismaValue::Int(size.try_into().unwrap_or(0))]))
}
