DEFINE TABLE OVERWRITE employee_of SCHEMAFULL
	TYPE RELATION FROM user TO organization
	PERMISSIONS
		FOR select WHERE in = $auth
			OR fn::organization::permissible($auth, out, "employees/view")
		FOR create WHERE fn::organization::permissible($auth, out, "employees/manage")
		FOR update, delete WHERE in != $auth
			AND fn::organization::permissible($auth, out, "employees/manage");

DEFINE FIELD OVERWRITE role ON employee_of TYPE record<organization_role>;

DEFINE FIELD OVERWRITE title ON employee_of TYPE option<string>;

DEFINE FIELD OVERWRITE status ON employee_of TYPE string
	ASSERT $value IN ["invited", "active", "suspended", "resigned", "terminated"]
	DEFAULT "active";

DEFINE FIELD OVERWRITE joined_at ON employee_of TYPE datetime DEFAULT time::now();

DEFINE INDEX OVERWRITE employee_of_by_account ON employee_of FIELDS in;
DEFINE INDEX OVERWRITE employee_of_by_organization ON employee_of FIELDS out;
DEFINE INDEX OVERWRITE employee_of_unique ON employee_of FIELDS in, out UNIQUE;

DEFINE EVENT OVERWRITE guard_last_admin ON employee_of WHEN $event IN ["DELETE", "UPDATE"] THEN {
	LET $org = IF $event = "DELETE" THEN $before.out ELSE $after.out END;
	LET $admins = (SELECT VALUE id FROM employee_of
		WHERE out = $org AND status = "active"
			AND fn::organization::role::contains(role, "organization/manage"));
	IF array::len($admins) = 0 { THROW "Organization needs at least one administrator" };
};
