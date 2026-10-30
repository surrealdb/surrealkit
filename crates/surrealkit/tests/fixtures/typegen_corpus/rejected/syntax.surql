-- Rejected at parse time on 3.2.4 and 3.3.0. Each statement documents a spelling
-- typegen must not expect to see in INFO output.

-- `range` takes no type parameter.
DEFINE FIELD r ON k TYPE range<int>;
-- `feature` is not a geometry kind (the kinds are point, line, polygon, multipoint,
-- multiline, multipolygon and collection).
DEFINE FIELD g ON k TYPE geometry<feature>;
-- FLEXIBLE only applies to kinds containing `object`, and must follow TYPE.
DEFINE FIELD s ON k TYPE string FLEXIBLE;
DEFINE FIELD o ON k FLEXIBLE TYPE object;
-- `literal` is not a kind name.
DEFINE FIELD l ON k TYPE literal;
-- DEFER is not an index clause.
DEFINE INDEX i ON k FIELDS a DEFER;
-- WITH REFRESH must come before WITH JWT.
DEFINE ACCESS a ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM person) WITH JWT ALGORITHM HS512 KEY 'k' WITH REFRESH;
-- DEFINE MODEL is not a statement without the ML feature.
DEFINE MODEL ml::sentiment<1.0.0>;
