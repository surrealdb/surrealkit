-- The schema this example generates Effect modules from. `surrealkit compile`
-- applies it to a throwaway in-memory engine; nothing connects to a database.

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string ASSERT string::len($value) >= 1;
DEFINE FIELD email ON person TYPE string VALUE string::lowercase($value);
DEFINE FIELD nickname ON person TYPE option<string>;
DEFINE FIELD bio ON person TYPE string | null;
DEFINE FIELD age ON person TYPE option<int> ASSERT $value = NONE OR ($value >= 0 AND $value <= 150);
DEFINE FIELD role ON person TYPE 'member' | 'admin' DEFAULT 'member';
DEFINE FIELD created_at ON person TYPE datetime DEFAULT time::now() READONLY;
DEFINE FIELD updated_at ON person TYPE datetime DEFAULT ALWAYS time::now();
DEFINE FIELD display ON person TYPE string COMPUTED string::concat(name, ' <', email, '>');
DEFINE FIELD tags ON person TYPE set<string> DEFAULT <set<string>> [];
DEFINE FIELD balance ON person TYPE decimal DEFAULT 0dec;

DEFINE TABLE team SCHEMAFULL;
DEFINE FIELD name ON team TYPE string;
DEFINE FIELD meta ON team TYPE option<object> FLEXIBLE;

DEFINE TABLE member_of TYPE RELATION IN person OUT team ENFORCED SCHEMAFULL;
DEFINE FIELD since ON member_of TYPE datetime DEFAULT time::now() READONLY;
DEFINE FIELD lead ON member_of TYPE bool DEFAULT false;

DEFINE FUNCTION fn::greet($name: string, $greeting: option<string>) -> string {
	RETURN ($greeting ?? 'hello') + ' ' + $name;
};
