DEFINE TABLE OVERWRITE team SCHEMAFULL
	PERMISSIONS
		FOR select WHERE fn::team::member_of($auth, id, NONE)
			OR fn::organization::permissible($auth, organization, "team/view")
		FOR create WHERE fn::organization::permissible($auth, organization, "team/create")
			OR fn::organization::permissible($auth, organization, "team/manage")
		FOR update WHERE fn::organization::permissible($auth, organization, "team/update")
			OR fn::organization::permissible($auth, organization, "team/manage")
		FOR delete WHERE fn::organization::permissible($auth, organization, "team/delete")
			OR fn::organization::permissible($auth, organization, "team/manage");

DEFINE FIELD OVERWRITE name ON team TYPE string;

DEFINE FIELD OVERWRITE parent ON team TYPE option<record<team>>;

DEFINE FIELD OVERWRITE organization ON team TYPE record<organization>
	REFERENCE ON DELETE CASCADE;

DEFINE FIELD OVERWRITE created_at ON team TYPE datetime DEFAULT time::now();

DEFINE INDEX OVERWRITE team_org_name_unique ON team FIELDS organization, name UNIQUE;

DEFINE FIELD OVERWRITE teams ON organization COMPUTED <~team;
