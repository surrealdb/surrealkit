-- Typegen golden corpus, module 06: record links, REFERENCE ON DELETE strategies and
-- reverse lookups. SurrealDB 3.x has no `references<T>` kind; reverse references
-- are COMPUTED `<~` lookups (see rejected/references-kind.surql).

DEFINE FIELD manager ON person TYPE option<record<person>> REFERENCE ON DELETE UNSET;
DEFINE FIELD team ON person TYPE option<record<team>> REFERENCE ON DELETE REJECT;
DEFINE FIELD org ON person TYPE option<record<org>> REFERENCE ON DELETE CASCADE;
DEFINE FIELD buddy ON person TYPE option<record<person>> REFERENCE ON DELETE IGNORE;
DEFINE FIELD mentor ON person TYPE option<record<person>>
	REFERENCE ON DELETE THEN (UPDATE $this SET flagged = true);
DEFINE FIELD watchers ON person TYPE array<record<person>> DEFAULT [] REFERENCE;

-- Reverse lookups: every referencing record, and only those through one field
DEFINE FIELD referenced_by ON person COMPUTED <~person;
DEFINE FIELD members ON team COMPUTED <~(person FIELD team);
