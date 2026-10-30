-- Typegen golden corpus, module 05: nested paths, array element paths and FLEXIBLE.

-- Declared children of the FLEXIBLE `address` object (defined in module 01, because a
-- FLEXIBLE field needs its table to exist): undeclared keys are kept
DEFINE FIELD address.city ON person TYPE string;
DEFINE FIELD address.zip ON person TYPE option<string>;
DEFINE FIELD address.geo ON person TYPE option<geometry<point>>;

-- A closed object with nested children two levels deep
DEFINE FIELD settings ON person TYPE object DEFAULT {};
DEFINE FIELD settings.theme ON person TYPE 'light' | 'dark' DEFAULT 'light';
DEFINE FIELD settings.notify ON person TYPE object DEFAULT {};
DEFINE FIELD settings.notify.email ON person TYPE bool DEFAULT true;

-- Array of objects: element fields via both `[*]` and `.*` spellings
DEFINE FIELD phones ON person TYPE array<object> DEFAULT [];
DEFINE FIELD phones[*].number ON person TYPE string;
DEFINE FIELD phones.*.kind ON person TYPE 'home' | 'work' | 'mobile';

-- Set of strings; the engine synthesises `tags.*`. The default is an explicit set cast:
-- 3.2.4 does not coerce an array literal into a set field (a plain `DEFAULT []` makes
-- every CREATE fail there), while 3.3.0 does. A bound such as `set<string, 10>` would
-- demand exactly ten elements (see semantics/collections.surql).
DEFINE FIELD tags ON person TYPE set<string> DEFAULT <set<string>> [];

-- A nested field whose parent `prefs` is never declared: the engine accepts it and
-- treats the parent as an implicit object.
DEFINE FIELD prefs.locale ON person TYPE option<string>;

-- A field with no TYPE clause (any value)
DEFINE FIELD notes ON person;

-- Schemaless table with one declared field
DEFINE FIELD label ON bag TYPE option<string>;
DEFINE FIELD name ON team TYPE string;
DEFINE FIELD name ON org TYPE string;
