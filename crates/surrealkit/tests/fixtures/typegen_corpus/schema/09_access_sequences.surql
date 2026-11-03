-- Typegen golden corpus, module 09: ACCESS methods, USER, and SEQUENCE.
--
-- Key material below is a fixture value. INFO redacts it, and schema document v2
-- must never carry access secrets, user password hashes or signing keys.

DEFINE ACCESS account ON DATABASE TYPE RECORD
	SIGNUP (CREATE person SET email = $email, name = $name)
	SIGNIN (SELECT * FROM person WHERE email = $email)
	WITH REFRESH
	WITH JWT ALGORITHM HS512 KEY 'fixture-record-signing-key'
	DURATION FOR GRANT 30d, FOR TOKEN 15m, FOR SESSION 12h
	COMMENT 'record users';
DEFINE ACCESS remote_jwt ON DATABASE TYPE JWT URL 'https://example.com/.well-known/jwks.json' DURATION FOR SESSION 2h;
DEFINE ACCESS shared_jwt ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'fixture-shared-key';
DEFINE ACCESS service ON DATABASE TYPE BEARER FOR RECORD DURATION FOR GRANT 30d, FOR TOKEN 15m;
DEFINE ACCESS operators ON DATABASE TYPE BEARER FOR USER;

DEFINE USER reader ON DATABASE PASSWORD 'fixture-reader-pass' ROLES VIEWER DURATION FOR SESSION 1h COMMENT 'read only';

-- Sequences: declared batch/start/timeout, and defaults. Allocation state is not schema.
DEFINE SEQUENCE order_number BATCH 100 START 1000 TIMEOUT 5s;
DEFINE SEQUENCE plain_seq;

DEFINE TABLE purchase SCHEMAFULL;
DEFINE FIELD number ON purchase TYPE int DEFAULT sequence::nextval('order_number') READONLY;
DEFINE FIELD buyer ON purchase TYPE record<person>;
DEFINE FIELD total ON purchase TYPE decimal ASSERT $value >= 0dec;
