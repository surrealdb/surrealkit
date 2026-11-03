-- Typegen golden corpus, module 05 (3.x spelling): a nested field whose parent `prefs` is
-- never declared. 3.2 and 3.3 accept it and treat the parent as an implicit object; 4.0
-- refuses it, see 05_declared_parent.v4.0.surql.

DEFINE FIELD prefs.locale ON person TYPE option<string>;
