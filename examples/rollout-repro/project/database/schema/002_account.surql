DEFINE TABLE account SCHEMAFULL;

DEFINE FIELD owner ON account
	TYPE record<person>;

DEFINE FIELD balance ON account
	TYPE number
	DEFAULT 0;
