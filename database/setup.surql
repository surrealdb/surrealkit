DEFINE TABLE OVERWRITE __sync SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE path ON __sync
	TYPE string;

DEFINE FIELD OVERWRITE hash ON __sync
	TYPE string;

DEFINE FIELD OVERWRITE synced_at ON __sync
	TYPE datetime
	DEFAULT time::now();

DEFINE INDEX OVERWRITE by_path ON __sync
	FIELDS path
	UNIQUE;

DEFINE TABLE OVERWRITE __sync_meta SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE key ON __sync_meta
	TYPE string;

DEFINE FIELD OVERWRITE value ON __sync_meta
	TYPE any;

DEFINE FIELD OVERWRITE updated_at ON __sync_meta
	TYPE datetime
	DEFAULT time::now();

DEFINE INDEX OVERWRITE by_key ON __sync_meta
	FIELDS key
	UNIQUE;

DEFINE TABLE OVERWRITE __entity SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE kind ON __entity
	TYPE string;

DEFINE FIELD OVERWRITE scope ON __entity
	TYPE option<string>;

DEFINE FIELD OVERWRITE name ON __entity
	TYPE string;

DEFINE FIELD OVERWRITE source_path ON __entity
	TYPE string;

DEFINE FIELD OVERWRITE statement_hash ON __entity
	TYPE string;

DEFINE FIELD OVERWRITE file_hash ON __entity
	TYPE string;

DEFINE FIELD OVERWRITE active_rollout_id ON __entity
	TYPE option<string>;

DEFINE FIELD OVERWRITE state ON __entity
	TYPE string
	DEFAULT "active";

DEFINE FIELD OVERWRITE updated_at ON __entity
	TYPE datetime
	DEFAULT time::now();

DEFINE INDEX OVERWRITE by_entity_key ON __entity
	FIELDS kind, scope, name
	UNIQUE;

DEFINE TABLE OVERWRITE __rollout SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE id ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE name ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE manifest_path ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE manifest_checksum ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE source_schema_hash ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE target_schema_hash ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE status ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE source_entities ON __rollout
	TYPE any;

DEFINE FIELD OVERWRITE target_entities ON __rollout
	TYPE any;

DEFINE FIELD OVERWRITE started_at ON __rollout
	TYPE datetime
	DEFAULT time::now();

DEFINE FIELD OVERWRITE completed_at ON __rollout
	TYPE option<datetime>;

DEFINE FIELD OVERWRITE updated_at ON __rollout
	TYPE datetime
	DEFAULT time::now();

DEFINE FIELD OVERWRITE last_error ON __rollout
	TYPE option<string>;

DEFINE INDEX OVERWRITE by_rollout_id ON __rollout
	FIELDS id
	UNIQUE;

DEFINE TABLE OVERWRITE __rollout_step SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE rollout_id ON __rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE step_id ON __rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE phase ON __rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE kind ON __rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE checksum ON __rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE status ON __rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE started_at ON __rollout_step
	TYPE datetime
	DEFAULT time::now();

DEFINE FIELD OVERWRITE finished_at ON __rollout_step
	TYPE option<datetime>;

DEFINE FIELD OVERWRITE error ON __rollout_step
	TYPE option<string>;

DEFINE INDEX OVERWRITE by_rollout_step ON __rollout_step
	FIELDS rollout_id, step_id
	UNIQUE;

DEFINE TABLE OVERWRITE __lock SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE key ON __lock
	TYPE string;

DEFINE FIELD OVERWRITE owner ON __lock
	TYPE string;

DEFINE FIELD OVERWRITE created_at ON __lock
	TYPE datetime
	DEFAULT time::now();

DEFINE INDEX OVERWRITE by_lock_key ON __lock
	FIELDS key
	UNIQUE;
