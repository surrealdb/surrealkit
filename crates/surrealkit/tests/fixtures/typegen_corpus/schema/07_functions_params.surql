-- Typegen golden corpus, module 07: DEFINE FUNCTION and DEFINE PARAM.

-- Typed args, trailing optional args, declared return, permissions and comment
DEFINE FUNCTION fn::greet($name: string, $greeting: option<string>, $times: option<int>) -> string {
	RETURN ($greeting ?? 'hello') + ' ' + $name;
} PERMISSIONS FULL COMMENT 'greets someone';

-- No declared return type
DEFINE FUNCTION fn::double($x: int) {
	RETURN $x * 2;
};

-- No args, conditional permission
DEFINE FUNCTION fn::now() -> datetime {
	RETURN time::now();
} PERMISSIONS WHERE $auth.admin = true;

-- Nested name, record/set args, collection return, PERMISSIONS NONE
DEFINE FUNCTION fn::doc::related($d: record<doc>, $tags: set<string>) -> array<record<doc>> {
	RETURN [$d];
} PERMISSIONS NONE;

-- Accepted by the engine although an optional arg precedes a required one; only
-- trailing option<T> args can be omitted by callers, so typegen must diagnose this.
DEFINE FUNCTION fn::optional_first($a: option<int>, $b: int) -> int {
	RETURN $b;
};

-- Used by an analyzer in module 08
DEFINE FUNCTION fn::prep($s: string) -> string {
	RETURN string::lowercase($s);
};

-- Params: constant string, constant object (with a decimal), and an expression.
-- The engine evaluates a param's VALUE once, at definition time, and INFO returns the
-- result (`$derived` reads back as `3`). A non-deterministic expression such as
-- `time::now()` would therefore put a timestamp into every introspection, so the
-- corpus uses a deterministic one.
DEFINE PARAM $app_name VALUE 'corpus' COMMENT 'application name';
DEFINE PARAM $limits VALUE { max: 10, ratio: 0.5dec } PERMISSIONS NONE;
DEFINE PARAM $derived VALUE math::max([1, 2, 3]);
