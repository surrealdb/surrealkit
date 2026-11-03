-- Typegen golden corpus, module 30: relation features introduced in SurrealDB 3.3.
-- 3.2.x rejects LIGHTWEIGHT and INLINE at parse time. Gated modules live outside
-- `schema/` because SurrealKit's own test suite applies every file under a `schema/`
-- directory, alone, to a default-capability engine on every CI lane (3.2 and 3.3).

-- A LIGHTWEIGHT relation synthesises its edge record and allows no fields,
-- indexes, events or ordinary updates. 3.3.0 reports it as ENFORCED.
DEFINE TABLE knows TYPE RELATION IN person OUT person LIGHTWEIGHT;

-- An INLINE field is embedded into the edges' adjacency payloads. It needs an
-- existing TYPE RELATION table.
DEFINE TABLE rates TYPE RELATION IN person OUT doc SCHEMAFULL;
DEFINE FIELD weight ON rates TYPE option<float> INLINE;
