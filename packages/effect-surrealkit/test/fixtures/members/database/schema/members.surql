-- Field permissions and unique indexes, for the `members` golden: the row a
-- restricted session reads (`MemberVisible`) and lookups by a unique index.
-- Compile with `surrealkit compile` in this directory after a change.

DEFINE TABLE member SCHEMAFULL
	PERMISSIONS FOR select FULL FOR create, update, delete NONE;
DEFINE FIELD handle ON member TYPE string;
DEFINE FIELD region ON member TYPE string;
DEFINE FIELD number ON member TYPE int;
DEFINE FIELD email ON member TYPE string
	PERMISSIONS FOR select WHERE $auth.id = id;
DEFINE FIELD salary ON member TYPE int
	PERMISSIONS FOR select NONE;
DEFINE FIELD nickname ON member TYPE option<string>
	PERMISSIONS FOR select WHERE $auth.id = id;
DEFINE FIELD profile ON member TYPE object;
DEFINE FIELD profile.bio ON member TYPE string;
DEFINE FIELD profile.phone ON member TYPE string
	PERMISSIONS FOR select WHERE $auth.id = id;
DEFINE INDEX member_handle ON member FIELDS handle UNIQUE;
DEFINE INDEX member_region_number ON member FIELDS region, number UNIQUE;
DEFINE INDEX member_region ON member FIELDS region;
DEFINE INDEX member_bio ON member FIELDS profile.bio UNIQUE;

DEFINE TABLE viewer SCHEMAFULL
	PERMISSIONS FOR select WHERE id = $auth.id FOR create, update, delete NONE;
DEFINE FIELD name ON viewer TYPE string;

DEFINE ACCESS viewer ON DATABASE TYPE RECORD
	SIGNUP (CREATE viewer SET name = $name)
	SIGNIN (SELECT * FROM viewer WHERE name = $name)
	WITH JWT ALGORITHM HS512 KEY 'members-fixture-only';
