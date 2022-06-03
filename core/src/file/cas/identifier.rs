use super::checksum::generate_cas_id;
use crate::job::JobReportUpdate;
use crate::prisma::{file, PrismaClient};
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
use prisma_client_rust::{raw, Direction, Error};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::{fs, io};

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
			let mut file_path_to_file: Vec<(i32, i32, String)> = Vec::new();

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
				let mut next_file_id =
					match block_on(db._query_raw::<QueryRes>(raw!("SELECT MAX(id) id FROM files")))
					{
						Ok(rows) => rows[0].id.unwrap_or(0),
						Err(e) => panic!("Error querying for next file id: {}", e),
					};

				// raw values to be inserted into the database
				let mut values: Vec<PrismaValue> = Vec::new();

				// prepare unique file values for each file path
				for file_path in file_paths.iter() {
					next_file_id += 1;
					let file_id = next_file_id.clone();
					// get the values for this unique file
					match prepare_file_values(&file_id, &location_path, file_path) {
						Ok((cas_id, data)) => {
							// store relation of file path id to file id
							file_path_to_file.push((file_path.id, file_id, cas_id));
							// add values to raw query data
							values.extend(data);
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

				println!(
					"Inserting {} unique file records ({:?} values)",
					file_paths.len(),
					values.len()
				);

				let query = format!(
					"INSERT INTO files (id, cas_id, size_in_bytes) VALUES {} 
					ON CONFLICT (cas_id) DO NOTHING RETURNING id, cas_id",
					vec!["({}, {}, {})"; file_paths.len()].join(",")
				);

				// insert files
				let files: Vec<FileCreated> = block_on(db._query_raw(Raw::new(&query, values)))
					.unwrap_or_else(|e| {
						println!("Error inserting files: {}", e);
						Vec::new()
					});

				println!(
					"Assigning {} unique file ids to origin file_paths",
					files.len()
				);
				// assign unique file to file path
				for (file_path_id, file_id, cas_id) in file_path_to_file.iter() {
					match block_on(assign_file(db.clone(), file_path_id, file_id)) {
						Ok(_) => {}
						Err(_) => {
							// if there was already a file record for this cas_id we first search memory for the file id
							match file_path_to_file.iter().find(|(_, _, cid)| *cid == *cas_id) {
								Some((_, file_id, cas_id)) => {
									// we attempt again at assigning the file id to the file path
									match block_on(assign_file(db.clone(), file_path_id, file_id)) {
										Ok(_) => {}
										Err(_) => {
											// in this case there is still a conflict meaning this cas_id is already assigned
											// to another file from outside this process, now we fall back to getting the cas_id from the database
											println!("Couldn't find cas_id in memory, getting from database...");

											#[derive(Deserialize, Serialize, Debug)]
											struct FileIdOnly {
												id: Option<i32>,
											}
											let file = block_on(db._query_raw::<FileIdOnly>(raw!(
												"SELECT id FROM files WHERE cas_id = {}",
												PrismaValue::String(cas_id.clone())
											)))
											.unwrap();

											let id = file[0].id.unwrap();

											block_on(assign_file(db.clone(), file_path_id, &id))
												.unwrap();
										}
									}
								}
								None => {
									println!(
										"Error assigning file id {} to file path id {}",
										file_id, file_path_id
									);
								}
							}
						}
					}
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
						completed, task_count
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

pub async fn assign_file(
	db: Arc<PrismaClient>,
	file_path_id: &i32,
	file_id: &i32,
) -> Result<Option<file_path::Data>, Error> {
	db.file_path()
		.find_unique(file_path::id::equals(file_path_id.clone()))
		.update(vec![file_path::file_id::set(Some(file_id.clone()))])
		.exec()
		.await
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

	Ok((
		cas_id.clone(),
		[
			PrismaValue::Int(id.clone().into()),
			PrismaValue::String(cas_id),
			PrismaValue::Int(size.try_into().unwrap_or(0)),
		],
	))
}
