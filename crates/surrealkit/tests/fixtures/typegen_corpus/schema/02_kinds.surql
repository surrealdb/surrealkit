-- Typegen golden corpus, module 02: every SurrealDB 3.x value kind on one table.
--
-- Each field name says what it covers. The engine normalises some spellings on
-- the way back out (for example `option<T>` becomes `none | T`, float literals
-- gain an `f` suffix), and synthesises an element field `<name>.*` for every
-- array or set; the introspection snapshots record exactly what comes back.

DEFINE TABLE kinds SCHEMAFULL COMMENT 'one field per value kind';

-- Primitives
DEFINE FIELD k_any ON kinds TYPE any;
DEFINE FIELD k_none ON kinds TYPE none;
DEFINE FIELD k_null ON kinds TYPE null;
DEFINE FIELD k_bool ON kinds TYPE bool;
DEFINE FIELD k_int ON kinds TYPE int;
DEFINE FIELD k_float ON kinds TYPE float;
DEFINE FIELD k_decimal ON kinds TYPE decimal;
DEFINE FIELD k_number ON kinds TYPE number;
DEFINE FIELD k_string ON kinds TYPE string;
DEFINE FIELD k_datetime ON kinds TYPE datetime;
DEFINE FIELD k_duration ON kinds TYPE duration;
DEFINE FIELD k_uuid ON kinds TYPE uuid;
DEFINE FIELD k_bytes ON kinds TYPE bytes;
DEFINE FIELD k_regex ON kinds TYPE regex;
DEFINE FIELD k_range ON kinds TYPE range;
DEFINE FIELD k_function ON kinds TYPE function;

-- Objects: bare, literal (closed) and a tagged union of literal objects
DEFINE FIELD k_object ON kinds TYPE object;
DEFINE FIELD k_object_literal ON kinds TYPE { a: int, b: option<string>, c: { d: bool } };
DEFINE FIELD k_object_union ON kinds TYPE { kind: 'circle', r: float } | { kind: 'rect', w: float, h: float };
DEFINE FIELD k_object_flexible ON kinds TYPE object FLEXIBLE;

-- Arrays, tuples and sets, with and without bounds
DEFINE FIELD k_array ON kinds TYPE array;
DEFINE FIELD k_array_of ON kinds TYPE array<string>;
DEFINE FIELD k_array_bounded ON kinds TYPE array<int, 3>;
DEFINE FIELD k_array_nested ON kinds TYPE array<array<float>>;
DEFINE FIELD k_tuple ON kinds TYPE [string, int, option<bool>];
DEFINE FIELD k_set ON kinds TYPE set;
DEFINE FIELD k_set_of ON kinds TYPE set<string>;
DEFINE FIELD k_set_bounded ON kinds TYPE set<int, 5>;
DEFINE FIELD k_array_or_set ON kinds TYPE array<int, 2> | set<string>;

-- Records and tables
DEFINE FIELD k_record_any ON kinds TYPE record;
DEFINE FIELD k_record ON kinds TYPE record<person>;
DEFINE FIELD k_record_union ON kinds TYPE record<person | team>;
DEFINE FIELD k_table_any ON kinds TYPE table;
DEFINE FIELD k_table ON kinds TYPE table<person | team>;

-- Geometry: bare, one kind, every kind
DEFINE FIELD k_geometry ON kinds TYPE geometry;
DEFINE FIELD k_geometry_point ON kinds TYPE geometry<point>;
DEFINE FIELD k_geometry_all ON kinds TYPE geometry<point | line | polygon | multipoint | multiline | multipolygon | collection>;

-- Files (the kind needs no experimental capability; DEFINE BUCKET does)
DEFINE FIELD k_file ON kinds TYPE file;
DEFINE FIELD k_file_bucket ON kinds TYPE file<avatars>;
DEFINE FIELD k_file_buckets ON kinds TYPE file<avatars | docs>;

-- Optionality and nullability are different things
DEFINE FIELD k_option ON kinds TYPE option<string>;
DEFINE FIELD k_nullable ON kinds TYPE string | null;
DEFINE FIELD k_option_nullable ON kinds TYPE option<string | null>;
DEFINE FIELD k_option_record ON kinds TYPE option<record<person>>;
DEFINE FIELD k_option_array ON kinds TYPE option<array<record<team>>>;

-- Literal unions: strings, numbers (int, float, decimal), booleans, durations
DEFINE FIELD k_literal_strings ON kinds TYPE 'draft' | 'published' | 'archived';
DEFINE FIELD k_literal_double_quoted ON kinds TYPE "a" | "b";
DEFINE FIELD k_literal_numbers ON kinds TYPE -5 | 0 | 1.5 | 10dec;
DEFINE FIELD k_literal_mixed ON kinds TYPE 'auto' | 1 | true | 1d;
DEFINE FIELD k_primitive_union ON kinds TYPE int | string | datetime;
