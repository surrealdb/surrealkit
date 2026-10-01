-- Notes belong to the account that wrote them.
DEFINE TABLE note SCHEMAFULL
	PERMISSIONS
		FOR select, update, delete WHERE owner = $auth,
		FOR create WHERE $auth != NONE;
DEFINE FIELD owner ON note TYPE record<account> DEFAULT $auth READONLY;
DEFINE FIELD body ON note TYPE string;
DEFINE FIELD tag ON note TYPE option<string> ASSERT $value = NONE OR $value = /^[a-z][a-z0-9_-]*$/;
