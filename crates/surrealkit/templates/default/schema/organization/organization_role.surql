DEFINE TABLE OVERWRITE organization_role SCHEMAFULL
	PERMISSIONS
		FOR select WHERE fn::organization::permissible($auth, organization, "roles/read")
		FOR create, update, delete WHERE fn::organization::permissible($auth, organization, "roles/manage");

DEFINE FIELD OVERWRITE name ON organization_role TYPE string;

DEFINE FIELD OVERWRITE organization ON organization_role TYPE record<organization>
	REFERENCE ON DELETE CASCADE;

DEFINE FIELD OVERWRITE description ON organization_role TYPE option<string>;

DEFINE FIELD OVERWRITE kind ON organization_role TYPE string
	ASSERT $value IN ["system", "organization"]
	DEFAULT "organization";

DEFINE FIELD OVERWRITE sort_order ON organization_role TYPE number DEFAULT 0;

DEFINE FIELD OVERWRITE permissions ON organization_role TYPE array<record<organization_permission>>
	DEFAULT [];

DEFINE FIELD OVERWRITE created_by ON organization_role TYPE option<record<user>>;

DEFINE INDEX OVERWRITE organization_role_unique ON organization_role
	FIELDS organization, name UNIQUE;
