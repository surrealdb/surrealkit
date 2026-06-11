DEFINE TABLE OVERWRITE organization_delegation_grant SCHEMAFULL
	TYPE RELATION FROM organization TO organization
	PERMISSIONS
		FOR select WHERE fn::organization::permissible($auth, in, "organization/subsidiaries")
		FOR create, update, delete WHERE fn::organization::permissible($auth, in, "organization/subsidiaries")
			AND fn::organization::permissible($auth, out, "organization/subsidiaries");

DEFINE FIELD OVERWRITE subject ON organization_delegation_grant TYPE record;

DEFINE FIELD OVERWRITE role ON organization_delegation_grant TYPE record<organization_role>;

DEFINE FIELD OVERWRITE scope ON organization_delegation_grant TYPE string
	ASSERT $value IN ["organization", "unit"]
	DEFAULT "organization";

DEFINE FIELD OVERWRITE unit ON organization_delegation_grant TYPE option<record>;

DEFINE FIELD OVERWRITE created_at ON organization_delegation_grant TYPE datetime DEFAULT time::now();

DEFINE EVENT OVERWRITE grant_validation ON organization_delegation_grant
	WHEN $event = "CREATE" OR $event = "UPDATE" THEN {
	LET $relation = (SELECT VALUE id FROM has_subsidiary
		WHERE in = $after.in AND out = $after.out AND status = "active");
	IF $relation = NONE OR array::len($relation) = 0 { THROW "UNAUTHORIZED_SUBSIDIARY" };

	IF $after.role.organization != $after.out { THROW "DELEGATED_ROLE_NOT_IN_SUBSIDIARY" };

	IF $after.scope = "unit" AND $after.unit = NONE { THROW "DELEGATED_UNIT_SCOPE_NOT_PRESENT" };
};
