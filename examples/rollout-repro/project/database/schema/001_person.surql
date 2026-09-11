DEFINE TABLE person SCHEMAFULL;

DEFINE FIELD email ON person
	TYPE string
	ASSERT string::is_email($value);

-- The rollout-repro harness tightens this ASSERT to `$value >= 18` to reproduce
-- the "plan refuses a modified entity" case. Keep it as one statement on one
-- field: the diff is per-DEFINE-statement, so a single clause change is the
-- smallest possible reproduction.
DEFINE FIELD age ON person
	TYPE int
	ASSERT $value >= 0;
