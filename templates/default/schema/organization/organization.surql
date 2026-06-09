DEFINE TABLE OVERWRITE organization SCHEMAFULL
	PERMISSIONS
		FOR select WHERE fn::organization::employee($auth, id)
		FOR create WHERE $auth != NONE
		FOR update WHERE fn::organization::permissible($auth, id, "organization/manage")
		FOR delete NONE;

DEFINE FIELD OVERWRITE name ON organization TYPE string;

DEFINE FIELD OVERWRITE legal_name ON organization TYPE option<string>;

DEFINE FIELD OVERWRITE slug ON organization TYPE string
	ASSERT $value = string::slug($value);

DEFINE FIELD OVERWRITE logo ON organization TYPE option<string>;

DEFINE FIELD OVERWRITE owner ON organization TYPE record<user>
	DEFAULT $auth;

DEFINE FIELD OVERWRITE created_at ON organization TYPE datetime
	DEFAULT time::now();

DEFINE INDEX OVERWRITE organization_slug_unique ON organization FIELDS slug UNIQUE;

DEFINE EVENT OVERWRITE setup_organization ON organization WHEN $event = "CREATE" THEN {
	LET $owner_role = CREATE ONLY organization_role SET
		name = "Owner",
		organization = $after.id,
		description = "Owner of the organization",
		kind = "system",
		permissions = (SELECT VALUE id FROM organization_permission),
		created_by = $after.owner;

	LET $owner = $after.owner;
	LET $org = $after.id;
	RELATE $owner->employee_of->$org CONTENT {
		role: $owner_role.id,
		title: "Owner",
		status: "active"
	};
};
