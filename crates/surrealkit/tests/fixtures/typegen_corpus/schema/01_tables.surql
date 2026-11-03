-- Typegen golden corpus, module 01: every DEFINE TABLE shape typegen must understand.

-- Normal tables, explicit and implicit TYPE NORMAL, with each permission form:
-- FULL (true), NONE (false) and a WHERE expression, shared across operations.
DEFINE TABLE person TYPE NORMAL SCHEMAFULL
	PERMISSIONS
		FOR select FULL,
		FOR create, update WHERE $auth.id = id,
		FOR delete NONE
	COMMENT 'people who can sign in';
DEFINE TABLE team SCHEMAFULL PERMISSIONS FULL;
DEFINE TABLE org SCHEMAFULL COMMENT 'it\'s an "organisation"';

-- FLEXIBLE fields. Unlike other fields, `DEFINE FIELD … FLEXIBLE` fails with "The table
-- 'person' does not exist" when its table is not yet defined (3.2.4 and 3.3.0), so they
-- sit next to their tables. Their children are in module 05.
DEFINE FIELD address ON person TYPE object FLEXIBLE;
DEFINE FIELD meta ON team TYPE option<object> FLEXIBLE;

-- Relations: enforced union endpoints, FROM/TO spelling, schemaless edge, and an
-- unconstrained relation with no endpoint list.
DEFINE TABLE member_of TYPE RELATION IN person OUT team | org ENFORCED SCHEMAFULL;
DEFINE TABLE follows TYPE RELATION FROM person TO person SCHEMALESS;
DEFINE TABLE likes TYPE RELATION;

-- TYPE ANY and schemaless tables
DEFINE TABLE anything TYPE ANY SCHEMALESS;
DEFINE TABLE bag SCHEMALESS;

-- Change feeds, with and without the original record
DEFINE TABLE audit_log SCHEMAFULL CHANGEFEED 1d INCLUDE ORIGINAL;
DEFINE TABLE ledger SCHEMAFULL CHANGEFEED 7d;
DEFINE FIELD doc ON audit_log TYPE option<record<doc>>;
DEFINE FIELD note ON audit_log TYPE option<string>;
DEFINE FIELD amount ON ledger TYPE decimal;

-- A DROP table (accepts writes, stores nothing)
DEFINE TABLE sink DROP SCHEMAFULL;

-- Views: aggregate with GROUP BY, and a filtered projection. Views are
-- read-only and their row shape comes from the SELECT, not from fields.
DEFINE TABLE team_size AS SELECT count() AS total, team FROM person GROUP BY team;
DEFINE TABLE adults AS SELECT name, age FROM person WHERE age >= 18;
