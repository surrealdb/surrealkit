-- HAZARD — never apply to a database you want to keep using.
--
-- Accepted on 3.2.4 and 3.3.0. For an array-or-set field the engine synthesises an
-- element field `f.*` whose kind is the union of the element kinds; a bare `set`
-- contributes `any`, giving `int | any`. 3.2.4 stores and returns it. 3.3.0 stores it
-- but cannot re-parse `int | any` when it reloads the catalog, so EVERY later
-- statement in that database (including INFO) fails with:
--   Parse error: Unexpected token `ANY`, expected a kind name … int | any
-- The corpus uses `array<int, 2> | set<string>` (element kind `int | string`) instead.
DEFINE TABLE k SCHEMAFULL;
DEFINE FIELD f ON k TYPE array<int, 2> | set;
INFO FOR TABLE k;
