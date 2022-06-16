use prisma_client_rust_sdk::PrismaGenerator;

pub struct PrismaCRDTGenerator;

impl PrismaGenerator for PrismaCRDTGenerator {
	const NAME: &'static str = "Prisma CRDT Generator";
	const DEFAULT_OUTPUT: &'static str = "./prisma-crdt.rs";

	fn generate(args: prisma_client_rust_sdk::GenerateArgs) -> String {
		let mut out = String::new();

		for model in args.dml.models {
			out += &model.documentation.unwrap_or(String::new());
			out += "\n";
		}
		
		out
	}
}
