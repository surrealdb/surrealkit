-- Field-presence evidence for the matrix in docs/typegen/schema-document-v2.md.
--
-- Run statement by statement against a fresh database. Each statement is preceded by
-- its observed outcome on SurrealDB 3.2.4 and 3.3.0 (identical on both). `expect: OK`
-- lists the returned fields where they matter; `expect: ERR` quotes the engine error.
-- These are the seeds of the per-cell disposable-engine tests; they are not applied
-- as part of the schema corpus.

DEFINE TABLE m SCHEMAFULL;
DEFINE FIELD req ON m TYPE string;
DEFINE FIELD opt ON m TYPE option<string>;
DEFINE FIELD nul ON m TYPE string | null;
DEFINE FIELD dflt ON m TYPE string DEFAULT 'd';
DEFINE FIELD dalw ON m TYPE string DEFAULT ALWAYS 'a';
DEFINE FIELD optd ON m TYPE option<string> DEFAULT 'od';
DEFINE FIELD ro ON m TYPE string DEFAULT 'r' READONLY;
DEFINE FIELD roopt ON m TYPE option<string> READONLY;
DEFINE FIELD comp ON m TYPE string COMPUTED string::concat(req, '!');
DEFINE FIELD vin ON m TYPE string VALUE string::lowercase($value);
DEFINE FIELD vfall ON m TYPE string VALUE string::lowercase($value ?? 'X');
DEFINE FIELD vout ON m TYPE datetime VALUE d'2020-01-01T00:00:00Z';
DEFINE FIELD obj ON m TYPE option<object>;
DEFINE FIELD obj.a ON m TYPE option<int>;
DEFINE FIELD flex ON m TYPE option<object> FLEXIBLE;
DEFINE FIELD flex.a ON m TYPE option<int>;

-- expect: OK { dflt: 'd', dalw: 'a', optd: 'od', ro: 'r', comp: 'x!', vin: 'abc', vfall: 'x', vout: d'2020-01-01T00:00:00Z' }
CREATE m:1 CONTENT { req: 'x', nul: null, vin: 'ABC' };

-- TYPE T | null — required on create; NONE is not null.
-- expect: ERR Couldn't coerce value for field `nul` of `m:2`: Expected `string | null` but found `NONE`
CREATE m:2 CONTENT { req: 'x', vin: 'a' };

-- VALUE reading $value without a fallback — required on create.
-- expect: ERR Incorrect arguments for function string::lowercase(). Argument 1 was the wrong type. Expected `string` but found `NONE`
CREATE m:3 CONTENT { req: 'x', nul: null };

-- VALUE ignoring $value — a supplied value is silently replaced.
-- expect: OK { vout: d'2020-01-01T00:00:00Z' }
CREATE m:4 CONTENT { req: 'x', nul: null, vin: 'a', vout: d'1999-01-01T00:00:00Z' } RETURN vout;

-- COMPUTED supplied on create — silently ignored (typegen excludes it).
-- expect: OK { comp: 'x!' }
CREATE m:5 CONTENT { req: 'x', nul: null, vin: 'a', comp: 'zzz' } RETURN comp;

-- option<T> DEFAULT with explicit NONE on create — the default applies.
-- expect: OK { optd: 'od' }
CREATE m:6 CONTENT { req: 'x', nul: null, vin: 'a', optd: NONE } RETURN optd;

-- READONLY supplied on create — accepted.
-- expect: OK { ro: 'given' }
CREATE m:7 CONTENT { req: 'x', nul: null, vin: 'a', ro: 'given', roopt: 'p' } RETURN ro;

-- Undeclared top-level key on a schemafull table.
-- expect: ERR Found field 'nope', but no such field exists for table 'm'
CREATE m:8 CONTENT { req: 'x', nul: null, vin: 'a', nope: 1 };

-- Undeclared nested key in a non-FLEXIBLE object.
-- expect: ERR Found field 'obj.extra', but no such field exists for table 'm'
CREATE m:9 CONTENT { req: 'x', nul: null, vin: 'a', obj: { a: 1, extra: 2 } };

-- Undeclared nested keys in a FLEXIBLE object are kept.
-- expect: OK { flex: { a: 1, extra: { deep: true } } }
CREATE m:10 CONTENT { req: 'x', nul: null, vin: 'a', flex: { a: 1, extra: { deep: true } } } RETURN flex;

-- MERGE: TYPE T set to NONE.
-- expect: ERR Couldn't coerce value for field `req` of `m:1`: Expected `string` but found `NONE`
UPDATE m:1 MERGE { req: NONE };

-- MERGE: plain DEFAULT set to NONE — the default is NOT re-applied.
-- expect: ERR Couldn't coerce value for field `dflt` of `m:1`: Expected `string` but found `NONE`
UPDATE m:1 MERGE { dflt: NONE };

-- MERGE: DEFAULT ALWAYS set to NONE — the default is re-applied.
-- expect: OK { dalw: 'a' }
UPDATE m:1 MERGE { dalw: NONE } RETURN dalw;

-- MERGE: option<T> set to NONE — removed.
-- expect: OK { opt: 'o' }
UPDATE m:1 MERGE { opt: 'o' } RETURN opt;
-- expect: OK { opt: NONE }
UPDATE m:1 MERGE { opt: NONE } RETURN opt;

-- MERGE: READONLY unchanged value is accepted, a changed value or NONE is not.
-- expect: OK { ro: 'r' }
UPDATE m:1 MERGE { ro: 'r' } RETURN ro;
-- expect: ERR Found changed value for field `ro`, with record `m:1`, but field is readonly
UPDATE m:1 MERGE { ro: 'other' };
-- expect: ERR Found changed value for field `roopt`, with record `m:7`, but field is readonly
UPDATE m:7 MERGE { roopt: NONE };

-- MERGE: omitting a VALUE field keeps the stored value.
-- expect: OK { vin: 'abc' }
UPDATE m:1 MERGE { opt: 'z' } RETURN vin;

-- CONTENT: omitting a plain DEFAULT field — NOT re-applied, so it is required.
-- expect: ERR Couldn't coerce value for field `dflt` of `m:1`: Expected `string` but found `NONE`
UPDATE m:1 CONTENT { req: 'y', nul: null, vin: 'Q' };

-- CONTENT: omitting DEFAULT ALWAYS, READONLY, option<T> and VALUE-without-$value fields.
-- DEFAULT ALWAYS is re-applied, READONLY keeps the stored value, option<T> is removed
-- (even when it has a DEFAULT), VALUE is recomputed.
-- expect: OK { dalw: 'a', ro: 'r', optd: NONE, vout: d'2020-01-01T00:00:00Z' }
UPDATE m:1 CONTENT { req: 'y', nul: null, vin: 'Q', dflt: 'd' } RETURN dalw, ro, optd, vout;

-- CONTENT: omitting a VALUE field that reads $value without a fallback.
-- expect: ERR Incorrect arguments for function string::lowercase(). Argument 1 was the wrong type. Expected `string` but found `NONE`
UPDATE m:1 CONTENT { req: 'y', nul: null, dflt: 'd' };

-- CONTENT: a changed READONLY value.
-- expect: ERR Found changed value for field `ro`, with record `m:1`, but field is readonly
UPDATE m:1 CONTENT { req: 'y', nul: null, vin: 'Q', dflt: 'd', ro: 'changed' };

-- CONTENT: an `id` naming another record.
-- expect: ERR Found m:other for the `id` field, but a specific record has been specified
UPDATE m:1 CONTENT { id: m:other, req: 'y', nul: null, vin: 'Q', dflt: 'd' };

-- Relations: `in`/`out` are kept on CONTENT omission and ignored in MERGE.
DEFINE TABLE p SCHEMALESS;
DEFINE TABLE e TYPE RELATION IN p OUT p SCHEMAFULL;
DEFINE FIELD w ON e TYPE int DEFAULT 0;
CREATE p:a, p:b;
RELATE p:a->e:1->p:b CONTENT { w: 1 };
-- expect: OK { in: p:a, out: p:b, w: 2 }
UPDATE e:1 CONTENT { w: 2 } RETURN in, out, w;
-- expect: OK { in: p:a }
UPDATE e:1 MERGE { in: p:b } RETURN in;

-- Implicit parents (contract section 6): `pref` and `need` are never declared. The engine
-- keeps an empty object for an undeclared parent on every write, so the parent is always
-- present when selected; NONE resets it to `{}`, which fails when a child is required.
DEFINE TABLE ip SCHEMAFULL;
DEFINE FIELD pref.locale ON ip TYPE option<string>;
DEFINE FIELD need.x ON ip TYPE string;
-- expect: ERR Couldn't coerce value for field `need.x` of `ip:1`: Expected `string` but found `NONE`
CREATE ip:1 CONTENT {};
-- expect: OK { need: { x: 'v' }, pref: {} }
CREATE ip:2 CONTENT { need: { x: 'v' } } RETURN pref, need;
-- expect: OK { pref: { locale: 'en' } }
UPDATE ip:2 MERGE { pref: { locale: 'en' } } RETURN pref;
-- expect: OK { pref: {} }
UPDATE ip:2 MERGE { pref: NONE } RETURN pref;
-- expect: ERR Couldn't coerce value for field `need.x` of `ip:2`: Expected `string` but found `NONE`
UPDATE ip:2 MERGE { need: NONE };
-- expect: OK { need: { x: 'w' }, pref: {} }
UPDATE ip:2 CONTENT { need: { x: 'w' } } RETURN pref, need;

-- TYPE option<T> DEFAULT ALWAYS: the default is re-applied whenever the field becomes NONE,
-- so the field is resettable in MERGE (not removable) and optional in CONTENT.
DEFINE TABLE oa SCHEMAFULL;
DEFINE FIELD x ON oa TYPE option<string> DEFAULT ALWAYS 'z';
-- expect: OK { x: 'z' }
CREATE oa:1 CONTENT {} RETURN x;
-- expect: OK { x: 'z' }
CREATE oa:2 CONTENT { x: NONE } RETURN x;
-- expect: OK { x: 'y' }
UPDATE oa:1 MERGE { x: 'y' } RETURN x;
-- expect: OK { x: 'z' }
UPDATE oa:1 MERGE { x: NONE } RETURN x;
-- expect: OK { x: 'y' }
UPDATE oa:2 MERGE { x: 'y' } RETURN x;
-- expect: OK { x: 'z' }
UPDATE oa:2 CONTENT {} RETURN x;

-- A parent declared without TYPE whose child is typed: the engine treats it as an object
-- closed over its children, required when a child is required.
DEFINE TABLE up SCHEMAFULL;
DEFINE FIELD obj ON up;
DEFINE FIELD obj.a ON up TYPE int;
-- expect: ERR Couldn't coerce value for field `obj.a` of `up:1`: Expected `int` but found `NONE`
CREATE up:1 CONTENT {};
-- expect: ERR Couldn't coerce value for field `obj.a` of `up:2`: Expected `int` but found `'x'`
CREATE up:2 CONTENT { obj: { a: 'x' } };
-- expect: ERR Found field 'obj.b', but no such field exists for table 'up'
CREATE up:3 CONTENT { obj: { a: 1, b: 2 } };
-- expect: OK { obj: { a: 1 } }
CREATE up:4 CONTENT { obj: { a: 1 } } RETURN obj;
-- expect: OK { obj: { a: 2 } }
UPDATE up:4 MERGE { obj: { a: 2 } } RETURN obj;
-- expect: ERR Couldn't coerce value for field `obj.a` of `up:4`: Expected `int` but found `NONE`
UPDATE up:4 MERGE { obj: NONE };
-- expect: ERR Couldn't coerce value for field `obj.a` of `up:4`: Expected `int` but found `NONE`
UPDATE up:4 CONTENT {};
