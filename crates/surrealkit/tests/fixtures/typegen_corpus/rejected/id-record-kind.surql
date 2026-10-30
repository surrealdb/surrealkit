-- Rejected on 3.2.4 and 3.3.0: a record id key cannot itself be a record.
-- Engine error: Cannot use the `record<id_bad>` type on the `id` field, as that's not a valid record id key.
DEFINE TABLE id_bad SCHEMAFULL;
DEFINE FIELD id ON id_bad TYPE record<id_bad>;
