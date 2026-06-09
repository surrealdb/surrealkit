DEFINE TABLE OVERWRITE organization_unit SCHEMAFULL
	PERMISSIONS
		FOR select WHERE fn::organization::employee($auth, organization)
		FOR create, update, delete WHERE fn::organization::permissible($auth, organization, "organization/manage");

DEFINE FIELD OVERWRITE organization ON organization_unit TYPE record<organization>
	REFERENCE ON DELETE CASCADE;

DEFINE FIELD OVERWRITE parent ON organization_unit TYPE option<record<organization_unit>>;

DEFINE FIELD OVERWRITE name ON organization_unit TYPE string;

DEFINE FIELD OVERWRITE path ON organization_unit TYPE string DEFAULT "/";

DEFINE FIELD OVERWRITE kind ON organization_unit TYPE string
	ASSERT $value IN ["department", "region"]
	DEFAULT "department";

DEFINE FIELD OVERWRITE created_at ON organization_unit TYPE datetime DEFAULT time::now();

DEFINE INDEX OVERWRITE organization_unit_by_org ON organization_unit FIELDS organization;
DEFINE INDEX OVERWRITE organization_unit_path ON organization_unit FIELDS organization, path UNIQUE;

DEFINE FIELD OVERWRITE unit ON employee_of TYPE option<record<organization_unit>>
	ASSERT $value == NONE OR $value.organization = out;

DEFINE EVENT OVERWRITE set_path ON organization_unit
	WHEN $event = "CREATE" OR $event = "UPDATE" THEN {
	UPDATE organization_unit SET path = fn::organization::unit::path($value.id) WHERE id = $value.id;

	IF $event = "UPDATE" AND $before.parent != $after.parent {
		LET $old = $before.path;
		LET $new = (SELECT VALUE path FROM ONLY $value.id);
		UPDATE organization_unit
			SET path = string::replace(path, $old, $new)
			WHERE organization = $value.organization
				AND string::starts_with(path, $old)
				AND id != $value.id;
	};
};
