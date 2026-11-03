-- Access definitions on SurrealDB 3.3: every key INFO … STRUCTURE reports for an access.
-- `audience` and `context` are known keys that schema document v2 does not carry:
-- both readers report them as `definition-unsupported` warnings, never errors.
-- `authenticate` is preserved as an expression by both readers.
-- Key material is a fixture value; neither reader may carry it.

DEFINE TABLE account SCHEMAFULL;
DEFINE FIELD email ON account TYPE string;

DEFINE ACCESS issuer_jwt ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'fixture-access-key'
	AUDIENCE 'api', 'console'
	AUTHENTICATE { RETURN $auth }
	DURATION FOR SESSION 1h
	COMMENT 'audience and issuer';
DEFINE ACCESS remote_audience ON DATABASE TYPE JWT URL 'https://example.com/.well-known/jwks.json'
	AUDIENCE 'api'
	DURATION FOR SESSION 2h;
DEFINE ACCESS members ON DATABASE TYPE RECORD
	SIGNUP (CREATE account SET email = $email)
	SIGNIN (SELECT * FROM account WHERE email = $email)
	WITH REFRESH
	WITH JWT ALGORITHM HS256 KEY 'fixture-record-key' AUDIENCE 'members'
	AUTHENTICATE { RETURN $auth }
	CONTEXT { RETURN { tier: 'member' } }
	DURATION FOR GRANT 30d, FOR TOKEN 15m, FOR SESSION 12h;
DEFINE ACCESS keys ON DATABASE TYPE BEARER FOR RECORD DURATION FOR GRANT 30d, FOR TOKEN 15m;
