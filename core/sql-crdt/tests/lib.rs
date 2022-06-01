mod database;

use database::*;
use uhlc::{HLCBuilder, ID};

#[test]
fn shared_record_operations() {
	let mut db_0 = CRDTDatabase::new(
		0,
		HLCBuilder::new()
			.with_id(ID::try_from([0u8].as_slice()).unwrap())
			.build(),
	);
	let mut db_1 = CRDTDatabase::new(
		1,
		HLCBuilder::new()
			.with_id(ID::try_from([1u8].as_slice()).unwrap())
			.build(),
	);

	// Creates a shared record properly
	let tag1_op = db_0.create_tag(0, "Test".to_string(), "Blue".to_string());
	assert!(db_0.database().tag(0).is_some());

	db_1.apply_operation(tag1_op);
	assert!(db_1.database().tag(0).is_some());

	// Non-conflicting fields update properly
	let mut name_update = db_0.update_tag(0, vec![TagField::Name("Tag2".to_string())]);
	let mut color_update = db_1.update_tag(0, vec![TagField::Color("Red".to_string())]);

	db_1.apply_operation(name_update.swap_remove(0));
	assert_eq!(db_1.database().tag(0).unwrap().name, "Tag2");

	db_0.apply_operation(color_update.swap_remove(0));
	assert_eq!(db_0.database().tag(0).unwrap().color, "Red");

	// Multiple updates to same field results in last-write-wins
	let mut first_update = db_0.update_tag(0, vec![TagField::Name("TagFirstUpdate".to_string())]);
	let mut second_update = db_1.update_tag(0, vec![TagField::Name("TagSecondUpdate".to_string())]);

	db_1.apply_operation(first_update.swap_remove(0));
	assert_eq!(db_1.database().tag(0).unwrap().name, "TagSecondUpdate");

	db_0.apply_operation(second_update.swap_remove(0));
	assert_eq!(
		db_0.database().tag(0).unwrap().name,
		db_1.database().tag(0).unwrap().name
	);

	// Delete takes precedence over later updates
	let delete_op = db_0.delete_tag(0);
	let mut name_update = db_1.update_tag(0, vec![TagField::Name("Tag2".to_string())]);

	db_0.apply_operation(name_update.swap_remove(0));
	assert!(db_0.database().tag(0).is_none());

	db_1.apply_operation(delete_op);
	assert!(db_1.database().tag(0).is_none());
}
