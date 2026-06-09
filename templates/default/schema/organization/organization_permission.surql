DEFINE TABLE OVERWRITE organization_permission SCHEMAFULL
	PERMISSIONS
		FOR select WHERE $auth != NONE
		FOR create, update, delete NONE;

DEFINE FIELD OVERWRITE name ON organization_permission TYPE string;

DEFINE FIELD OVERWRITE description ON organization_permission TYPE option<string>;

DEFINE INDEX OVERWRITE organization_permission_name ON organization_permission FIELDS name UNIQUE;
