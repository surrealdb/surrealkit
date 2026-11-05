DEFINE TABLE OVERWRITE user SCHEMAFULL
	PERMISSIONS FOR select, update WHERE id = $auth.id, FOR create, delete NONE;
DEFINE FIELD OVERWRITE email ON user TYPE string ASSERT string::is_email($value);
DEFINE FIELD OVERWRITE pass ON user TYPE string;
DEFINE INDEX OVERWRITE email ON user FIELDS email UNIQUE;

DEFINE ACCESS OVERWRITE account ON DATABASE TYPE RECORD
	SIGNUP (CREATE user SET email = $email, pass = crypto::argon2::generate($pass))
	SIGNIN (SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass))
	DURATION FOR TOKEN 1h, FOR SESSION 12h;
