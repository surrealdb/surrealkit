DEFINE TABLE OVERWRITE organization_invitation SCHEMAFULL
	PERMISSIONS
		FOR create WHERE fn::organization::permissible($auth, organization_role.organization, "employees/manage")
		FOR select, update WHERE created_by = $auth;

DEFINE FIELD OVERWRITE organization_role ON organization_invitation TYPE record<organization_role>;

DEFINE FIELD OVERWRITE email ON organization_invitation TYPE string
	ASSERT string::is_email($value);

DEFINE FIELD OVERWRITE token ON organization_invitation TYPE string;

DEFINE FIELD OVERWRITE expires ON organization_invitation TYPE datetime;

DEFINE FIELD OVERWRITE teams ON organization_invitation TYPE array<record> DEFAULT [];

DEFINE FIELD OVERWRITE used_at ON organization_invitation TYPE option<datetime>;

DEFINE FIELD OVERWRITE used_by ON organization_invitation TYPE option<record<user>>;

DEFINE FIELD OVERWRITE created_by ON organization_invitation TYPE record<user>
	DEFAULT $auth;

DEFINE INDEX OVERWRITE organization_invitation_unique ON organization_invitation
	FIELDS email, organization_role UNIQUE;
DEFINE INDEX OVERWRITE organization_invitation_token ON organization_invitation
	FIELDS token UNIQUE;

DEFINE EVENT OVERWRITE on_accept ON organization_invitation
	WHEN $before.used_at != $after.used_at OR $before.used_by != $after.used_by THEN {
	IF $after.used_at = NONE OR $after.used_by = NONE { THROW "Invalid invitation" };

	LET $account = $after.used_by;
	LET $organization = (SELECT VALUE organization FROM ONLY $after.organization_role);

	RELATE $account->employee_of->$organization CONTENT {
		role: $after.organization_role,
		status: "active"
	};

	LET $employee = (SELECT VALUE id FROM employee_of
		WHERE in = $account AND out = $organization AND status = "active" LIMIT 1)[0];

	FOR $team IN ($after.teams ?? []) {
		RELATE $employee->member_of->$team;
	};
};
