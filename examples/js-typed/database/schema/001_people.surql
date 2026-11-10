-- The schema this example generates its query types from. `surrealkit compile` applies it to a
-- throwaway in-memory engine; nothing connects to a database.

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD email ON person TYPE string;
DEFINE FIELD age ON person TYPE option<int>;
DEFINE FIELD role ON person TYPE 'member' | 'admin';
