-- Typegen golden corpus, module 03: record-id key kinds via DEFINE FIELD id.
--
-- A table without an `id` field accepts any key kind. Declaring `id` constrains the
-- key; `record<...>` is not a valid key kind (see rejected/id-record-kind.surql).

DEFINE TABLE id_int SCHEMAFULL;
DEFINE FIELD id ON id_int TYPE int;

DEFINE TABLE id_string SCHEMAFULL;
DEFINE FIELD id ON id_string TYPE string;

DEFINE TABLE id_uuid SCHEMAFULL;
DEFINE FIELD id ON id_uuid TYPE uuid;

-- Compound keys: a tuple (array) key and an object key
DEFINE TABLE id_array SCHEMAFULL;
DEFINE FIELD id ON id_array TYPE [string, datetime];

DEFINE TABLE id_object SCHEMAFULL;
DEFINE FIELD id ON id_object TYPE { region: string, n: int };

DEFINE TABLE id_union SCHEMAFULL;
DEFINE FIELD id ON id_union TYPE int | string;

-- `DEFINE FIELD id` on an undefined table creates the table implicitly.
DEFINE FIELD id ON id_implicit TYPE int;
