use std::path::PathBuf;

pub const DEFAULT_ROOT_DIR: &str = "./database";

pub fn setup_surql_path(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("setup.surql")
}

pub fn schema_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("schema")
}

pub fn schemas_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("schemas")
}

pub fn named_schema_dir(folder: &str, schema: &str) -> PathBuf {
	schemas_dir(folder).join(schema)
}

pub fn rollouts_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("rollouts")
}

pub fn named_rollouts_dir(folder: &str, schema: &str) -> PathBuf {
	rollouts_dir(folder).join(schema)
}

pub fn state_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("snapshots")
}

pub fn named_state_dir(folder: &str, schema: &str) -> PathBuf {
	state_dir(folder).join(schema)
}

pub fn schema_snapshot_path(folder: &str) -> PathBuf {
	state_dir(folder).join("schema_snapshot.json")
}

pub fn catalog_snapshot_path(folder: &str) -> PathBuf {
	state_dir(folder).join("catalog_snapshot.json")
}

pub fn tests_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("tests")
}

pub fn suites_dir(folder: &str) -> PathBuf {
	tests_dir(folder).join("suites")
}

pub fn fixtures_dir(folder: &str) -> PathBuf {
	tests_dir(folder).join("fixtures")
}

pub fn seed_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("seed")
}

pub fn named_seed_dir(folder: &str, schema: &str) -> PathBuf {
	seed_dir(folder).join(schema)
}

pub fn seed_surql_path(folder: &str) -> PathBuf {
	seed_dir(folder).join("seed.surql")
}

pub fn types_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("types")
}

pub fn typegen_output_path(folder: &str) -> PathBuf {
	types_dir(folder).join("schema.json")
}

#[deprecated(
	note = "Deprecated path: `{folder}/seed.surql` (before the seed/ directory was introduced)"
)]
pub fn deprecated_seed_surql_path(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("seed.surql")
}
