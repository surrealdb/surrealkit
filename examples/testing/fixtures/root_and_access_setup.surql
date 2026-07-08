DEFINE TABLE OVERWRITE customer SCHEMAFULL
	PERMISSIONS
		FOR select WHERE $auth != NONE,
		FOR create, update WHERE $auth != NONE,
		FOR delete NONE;

DEFINE FIELD OVERWRITE name ON customer TYPE string;
DEFINE FIELD OVERWRITE age ON customer TYPE number;
DEFINE FIELD OVERWRITE joined_at ON customer TYPE datetime;
DEFINE FIELD OVERWRITE created_at ON customer TYPE datetime DEFAULT time::now();
DEFINE FIELD OVERWRITE tier ON customer TYPE string VALUE IF age >= 18 THEN "adult" ELSE "minor" END;

CREATE customer:alice CONTENT { name: 'Alice', age: 12, joined_at: <datetime>'2025-01-10T00:00:00Z' };

DEFINE TABLE OVERWRITE app_user SCHEMAFULL
	PERMISSIONS
		FOR select, create, update WHERE true,
		FOR delete NONE;

DEFINE FIELD OVERWRITE email ON app_user TYPE string;
DEFINE FIELD OVERWRITE pass ON app_user TYPE string;
DEFINE INDEX OVERWRITE app_user_email_unique ON TABLE app_user FIELDS email UNIQUE;

DEFINE ACCESS app_access ON DATABASE TYPE RECORD
	SIGNUP (
		CREATE app_user CONTENT {
			email: $email,
			pass: crypto::argon2::generate($password)
		}
	)
	SIGNIN (
		SELECT * FROM app_user
		WHERE email = $email
			AND crypto::argon2::compare(pass, $password)
	);

DEFINE TABLE OVERWRITE person SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD OVERWRITE name ON person TYPE string;

DEFINE TABLE OVERWRITE follows TYPE RELATION IN person OUT person
	SCHEMAFULL
	PERMISSIONS FULL;
DEFINE FIELD OVERWRITE since ON follows TYPE datetime;

DEFINE FUNCTION OVERWRITE fn::format_customer($name: string, $age: number) {
	RETURN string::concat($name, "#", <string>$age);
};

DEFINE API "/test"
	FOR get
		MIDDLEWARE
			api::res::body("json")
		THEN {
			{
				status: 200,
				body: {
					result: "OK"
				}
			}
		}
