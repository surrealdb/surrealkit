-- Accounts sign up and sign in with a passphrase.
DEFINE TABLE account SCHEMAFULL
	PERMISSIONS
		FOR select, update WHERE id = $auth,
		FOR create, delete NONE;
DEFINE FIELD email ON account TYPE string ASSERT string::is_email($value);
DEFINE FIELD passphrase ON account TYPE string;
DEFINE INDEX account_email ON account FIELDS email UNIQUE;

DEFINE ACCESS member ON DATABASE TYPE RECORD
	SIGNUP (CREATE account SET email = $email, passphrase = crypto::argon2::generate($passphrase))
	SIGNIN (SELECT * FROM account WHERE email = $email AND crypto::argon2::compare(passphrase, $passphrase))
	WITH JWT ALGORITHM HS512 KEY 'example-only-key'
	DURATION FOR SESSION 1h;
