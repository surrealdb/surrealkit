-- Typegen golden corpus, module 05 (4.0 spelling): a nested field and its parent `prefs`.
-- 4.0 requires the parent to be declared; 3.2 and 3.3 accept the undeclared form, see
-- 05_implicit_parent.v3.surql.

DEFINE FIELD prefs ON person TYPE object DEFAULT {};
DEFINE FIELD prefs.locale ON person TYPE option<string>;
