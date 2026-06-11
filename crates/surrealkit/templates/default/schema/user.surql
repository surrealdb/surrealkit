DEFINE TABLE OVERWRITE user SCHEMAFULL
	PERMISSIONS
		FOR select WHERE id = $auth.id
		FOR create, update, delete NONE;

DEFINE FIELD OVERWRITE email ON user TYPE string
	ASSERT string::is_email($value);

DEFINE FIELD OVERWRITE name ON user TYPE option<string>;

DEFINE FIELD OVERWRITE created_at ON user TYPE datetime
	DEFAULT time::now();

DEFINE INDEX OVERWRITE user_email_unique ON user FIELDS email UNIQUE;
