-- Objects without declared fields, and field names run together with an object's name.
--
-- On a SCHEMAFULL table SurrealDB 3.2.4 and 3.3.x refuse every undeclared key of an object
-- that is not FLEXIBLE ("Found field 'plain.k', but no such field exists"), at the top level,
-- under option, in array elements, and in a declared child of a FLEXIBLE parent: such an
-- object holds only {}. Schema document v2 reports it as the error `object-without-fields`.
-- The engine synthesises the element field `items.*` of an `array<object>`; the error names
-- it, and a FLEXIBLE array opens it.
-- FLEXIBLE objects, objects with declared fields and SCHEMALESS tables keep their keys.

DEFINE TABLE shapes SCHEMAFULL;
-- error object-without-fields at each of these:
DEFINE FIELD plain ON shapes TYPE option<object>;
DEFINE FIELD nullable ON shapes TYPE option<object | null>;
DEFINE FIELD items ON shapes TYPE option<array<object>>;
DEFINE FIELD parent ON shapes TYPE option<object> FLEXIBLE;
DEFINE FIELD parent.inner ON shapes TYPE option<object>;
-- no diagnostic:
DEFINE FIELD flex ON shapes TYPE option<object> FLEXIBLE;
DEFINE FIELD withkid ON shapes TYPE option<object>;
DEFINE FIELD withkid.k ON shapes TYPE option<string>;
DEFINE FIELD rows ON shapes TYPE option<array<object>>;
DEFINE FIELD rows.*.k ON shapes TYPE option<string>;
DEFINE FIELD openrows ON shapes TYPE option<array<object>> FLEXIBLE;
-- warning field-name-run-together at envelopealgorithm only:
DEFINE FIELD envelope ON shapes TYPE option<object>;
DEFINE FIELD envelope.algorithm ON shapes TYPE option<string>;
DEFINE FIELD envelopealgorithm ON shapes TYPE option<string>;
DEFINE FIELD envelope_kind ON shapes TYPE option<string>;
DEFINE FIELD envelopeKind ON shapes TYPE option<string>;

DEFINE TABLE loose SCHEMALESS;
-- no diagnostic: a SCHEMALESS table keeps the keys.
DEFINE FIELD plain ON loose TYPE option<object>;
