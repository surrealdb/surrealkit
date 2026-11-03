-- Typegen golden corpus, module 04: DEFINE FIELD clauses on `person`.
--
-- One field per row of the field-presence matrix in docs/typegen/schema-document-v2.md,
-- plus every ASSERT shape the recogniser must (and must not) understand.

-- TYPE T / option<T> / T | null
DEFINE FIELD name ON person TYPE string
	ASSERT string::len($value) >= 1 AND string::len($value) <= 80;
DEFINE FIELD nickname ON person TYPE option<string>;
DEFINE FIELD bio ON person TYPE string | null;

-- VALUE that reads $value (an input normaliser) and one that ignores it (a producer)
DEFINE FIELD email ON person TYPE string
	VALUE string::lowercase($value)
	ASSERT string::is_email($value);
DEFINE FIELD touched_at ON person TYPE datetime VALUE time::now();
DEFINE FIELD slug ON person TYPE string VALUE string::slug(name);

-- DEFAULT, DEFAULT ALWAYS, READONLY, and DEFAULT ALWAYS combined with VALUE
DEFINE FIELD role ON person TYPE string DEFAULT 'member'
	ASSERT $value IN ['member', 'admin', 'owner'];
DEFINE FIELD created_at ON person TYPE datetime DEFAULT time::now() READONLY;
DEFINE FIELD updated_at ON person TYPE datetime DEFAULT ALWAYS time::now();
DEFINE FIELD handle ON person TYPE string DEFAULT ALWAYS 'anon' VALUE string::trim($value);
DEFINE FIELD flagged ON person TYPE bool DEFAULT false;

-- OVERWRITE replaces the definition above. (A later `DEFINE FIELD IF NOT EXISTS flagged`
-- is deliberately absent: raw SurrealQL would ignore it, but SurrealKit applies every
-- non-sequence DEFINE as OVERWRITE, so the last definition would win. A module that
-- defines an entity twice is ambiguous; see `definition-duplicate`.)
DEFINE FIELD OVERWRITE flagged ON person TYPE bool DEFAULT false COMMENT 'overwritten once';

-- COMPUTED, with and without a declared TYPE
DEFINE FIELD display ON person COMPUTED string::concat(name, ' <', email, '>');
DEFINE FIELD display_typed ON person TYPE string COMPUTED string::concat(name, '!');

-- Recognisable assertions: numeric ranges (int, float, duration), membership,
-- string length, array length, email. And unrecognisable ones.
DEFINE FIELD age ON person TYPE option<int> ASSERT $value = NONE OR ($value >= 0 AND $value <= 150);
DEFINE FIELD score ON person TYPE float ASSERT $value > 0.0 AND $value < 100.0;
DEFINE FIELD session_ttl ON person TYPE duration DEFAULT 1h ASSERT $value >= 1m AND $value <= 1d;
DEFINE FIELD tier ON person TYPE string DEFAULT 'free' ASSERT $value INSIDE ['free', 'pro'];
DEFINE FIELD pins ON person TYPE array<string> DEFAULT [] ASSERT array::len($value) <= 3;
DEFINE FIELD shout ON person TYPE string DEFAULT 'HI'
	ASSERT $value = string::uppercase($value) OR $value CONTAINS '!';
DEFINE FIELD pattern ON person TYPE option<string> ASSERT $value = NONE OR $value = /^[a-z]+$/;

-- Field permissions and a namespaced policy comment (carried verbatim; never parsed by SurrealKit)
DEFINE FIELD ssn ON person TYPE option<string>
	PERMISSIONS
		FOR select WHERE $auth.admin = true,
		FOR create FULL,
		FOR update NONE
	COMMENT '@acme:policy {"classification":"restricted","archive":"encrypt"}';

-- Clause text containing keywords, semicolons and escaped quotes inside strings
DEFINE FIELD motto ON person TYPE string DEFAULT 'TYPE; VALUE \'quoted\' DEFAULT' COMMENT 'semi; colon "and" \'quotes\'';

-- A quoted identifier containing a dot (one segment, not a nested path)
DEFINE FIELD `odd.name` ON person TYPE option<string>;

-- Relation edge fields: `in`/`out` are implicit; extra edge content is declared
DEFINE FIELD since ON member_of TYPE datetime DEFAULT time::now() READONLY;
DEFINE FIELD role ON member_of TYPE 'member' | 'lead' DEFAULT 'member';
