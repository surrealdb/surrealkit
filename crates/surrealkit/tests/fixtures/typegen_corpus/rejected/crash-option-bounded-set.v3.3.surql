-- HAZARD — this terminates a SurrealDB 3.3.0 server process. Never run it against an
-- engine you need; use a throwaway container.
--
-- Writing an ARRAY that contains duplicates to an `option<set<T, N>>` field, where the
-- deduplicated length differs from N, makes 3.3.0 panic and abort (container exit 139):
--   thread 'surrealdb-worker' panicked at surrealdb/expr/src/val/value/convert/coerce.rs:738:43:
--   If can_coerce_to_kind returns true then coerce_to_kind must not error:
--   InvalidLength { len: 2, into: "set<int,3>" }
-- The same write to a non-option `set<int, 3>` field, and `RETURN <set<int, 3>> [3, 1, 1]`,
-- fail cleanly. 3.2.4 rejects all three cleanly because it does not coerce arrays into sets.
DEFINE TABLE a SCHEMAFULL;
DEFINE FIELD st ON a TYPE option<set<int, 3>>;
CREATE a:1 SET st = [3, 1, 1];
