DEFINE TABLE OVERWRITE member_of SCHEMAFULL
	TYPE RELATION FROM employee_of TO team
	PERMISSIONS
		FOR select WHERE in.in = $auth
			OR fn::organization::permissible($auth, out.organization, "team/view")
		FOR create, update, delete WHERE fn::organization::permissible($auth, out.organization, "team/manage")
			OR fn::team::member_of($auth, out, "maintainer");

DEFINE FIELD OVERWRITE role ON member_of TYPE string
	ASSERT $value IN ["maintainer", "member"]
	DEFAULT "member";

DEFINE FIELD OVERWRITE joined_at ON member_of TYPE datetime DEFAULT time::now();

DEFINE INDEX OVERWRITE member_of_by_employee ON member_of FIELDS in;
DEFINE INDEX OVERWRITE member_of_by_team ON member_of FIELDS out;
DEFINE INDEX OVERWRITE member_of_unique ON member_of FIELDS in, out UNIQUE;
