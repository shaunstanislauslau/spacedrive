use std::env;

use prisma_client_rust_sdk::execute;
use prisma_crdt::generator::PrismaCRDTGenerator;

fn main() {
	let args = env::args();

	let args = args.skip(1).collect();

	execute::<PrismaCRDTGenerator>(&args);
}
