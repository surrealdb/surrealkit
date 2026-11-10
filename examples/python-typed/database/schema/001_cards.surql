-- The schema this example generates Python types from. `surrealkit compile` applies it to a
-- throwaway in-memory engine; nothing connects to a database.

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;

DEFINE TABLE card SCHEMAFULL;
DEFINE FIELD title ON card TYPE string;
DEFINE FIELD status ON card TYPE 'todo' | 'done';
DEFINE FIELD due ON card TYPE option<datetime>;
DEFINE FIELD owner ON card TYPE record<person>;
DEFINE FIELD tags ON card TYPE set<string>;
