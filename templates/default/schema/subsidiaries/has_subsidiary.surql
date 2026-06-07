DEFINE TABLE OVERWRITE has_subsidiary SCHEMAFULL
	TYPE RELATION FROM organization TO organization
	PERMISSIONS
		FOR select WHERE fn::organization::permissible($auth, in, "organization/subsidiaries")
			OR fn::organization::permissible($auth, out, "organization/subsidiaries")
		FOR create, update, delete WHERE fn::organization::permissible($auth, in, "organization/subsidiaries")
			AND fn::organization::permissible($auth, out, "organization/subsidiaries");

DEFINE FIELD OVERWRITE status ON has_subsidiary TYPE string
	ASSERT $value IN ["active", "inactive"]
	DEFAULT "active";

DEFINE INDEX OVERWRITE has_subsidiary_by_parent ON has_subsidiary FIELDS in;
DEFINE INDEX OVERWRITE has_subsidiary_by_child ON has_subsidiary FIELDS out;
DEFINE INDEX OVERWRITE has_subsidiary_unique ON has_subsidiary FIELDS in, out UNIQUE;
