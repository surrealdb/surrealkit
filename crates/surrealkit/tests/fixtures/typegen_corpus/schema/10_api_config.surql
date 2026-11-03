-- Typegen golden corpus, module 10: DEFINE API and DEFINE CONFIG.
-- Neither needs an experimental capability on 3.2.4 or 3.3.0.

DEFINE API "/greet/:name" FOR get THEN {
	RETURN { status: 200, body: fn::greet($request.params.name) };
};
DEFINE API "/docs" FOR get, post MIDDLEWARE api::timeout(1s) PERMISSIONS FULL THEN {
	RETURN { status: 200 };
};
DEFINE CONFIG GRAPHQL AUTO;
DEFINE CONFIG API MIDDLEWARE api::timeout(5s);
