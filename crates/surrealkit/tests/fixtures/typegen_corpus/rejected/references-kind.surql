-- Rejected at parse time on 3.2.4 and 3.3.0: the 2.x `references` kind no longer exists.
-- Use a COMPUTED `<~table` lookup instead (see schema/06_references.surql).
DEFINE TABLE r SCHEMAFULL;
DEFINE FIELD back ON r TYPE references;
DEFINE FIELD back_typed ON r TYPE references<person>;
