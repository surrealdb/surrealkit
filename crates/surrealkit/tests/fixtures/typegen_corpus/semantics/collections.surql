-- Collection-kind evidence for the kind tree in docs/typegen/schema-document-v2.md.
--
-- Same format as field_presence.surql; `expect[3.2]:` / `expect[3.3]:` apply to one
-- engine minor only. Observed on SurrealDB 3.2.4 and 3.3.0.

DEFINE TABLE c SCHEMAFULL;
DEFINE FIELD arr ON c TYPE option<array<int, 3>>;
DEFINE FIELD st ON c TYPE option<set<int, 3>>;
DEFINE FIELD s ON c TYPE option<set<int>>;

-- `array<T, N>` demands exactly N elements: fewer and more are both rejected.
-- expect: OK { arr: [1, 2, 3] }
CREATE c:1 SET arr = [1, 2, 3] RETURN arr;
-- expect: ERR Couldn't coerce value for field `arr` of `c:2`: Expected `none | array<int, 3>` but found `[1, 2]`
CREATE c:2 SET arr = [1, 2];
-- expect: ERR Couldn't coerce value for field `arr` of `c:3`: Expected `none | array<int, 3>` but found `[1, 2, 3, 4]`
CREATE c:3 SET arr = [1, 2, 3, 4];

-- `set<T, N>` also demands exactly N (distinct) elements.
-- expect: OK { st: [1, 2, 3] }
CREATE c:4 SET st = <set<int>> [3, 2, 1] RETURN st;
-- expect: ERR Couldn't coerce value for field `st` of `c:5`: Expected `none | set<int, 3>` but found `{1, 2}`
CREATE c:5 SET st = <set<int>> [1, 2];

-- An array literal written to a set field: 3.2 rejects it, 3.3 coerces it
-- (deduplicated and sorted).
-- expect[3.2]: ERR Couldn't coerce value for field `s` of `c:6`: Expected `none | set<int>` but found `[5, 4, 5]`
-- expect[3.3]: OK { s: [4, 5] }
CREATE c:6 SET s = [5, 4, 5] RETURN s;

-- Explicit set casts behave the same on both: deduplicated and sorted.
-- expect: OK [1, 3]
RETURN <set<int>> [3, 1, 1];

-- 3.3.0 crash: see rejected/crash-option-bounded-set.v3.3.surql (not run here).
