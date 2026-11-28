-- The schema of the scripts test/block-engine.test.ts runs as blocks: accepting a
-- team invite, and paying from a wallet with an idempotency key. The engine
-- suite applies this text; test/golden/scripts is generated from its compiled
-- document (schema.v2.json, `surrealkit compile` run in this directory).

DEFINE TABLE account SCHEMAFULL;
DEFINE FIELD name ON account TYPE string;

DEFINE TABLE team SCHEMAFULL;
DEFINE FIELD name ON team TYPE string;
DEFINE FIELD seats ON team TYPE int;

DEFINE TABLE invite SCHEMAFULL;
DEFINE FIELD team ON invite TYPE record<team>;
DEFINE FIELD role ON invite TYPE 'member' | 'admin';
DEFINE FIELD status ON invite TYPE 'open' | 'accepted' | 'revoked';
DEFINE FIELD expires_at ON invite TYPE datetime;

DEFINE TABLE membership SCHEMAFULL;
DEFINE FIELD team ON membership TYPE record<team>;
DEFINE FIELD account ON membership TYPE record<account>;
DEFINE FIELD role ON membership TYPE 'member' | 'admin';
DEFINE FIELD key ON membership TYPE string;
DEFINE INDEX membership_key ON membership FIELDS key UNIQUE;

DEFINE TABLE wallet SCHEMAFULL;
DEFINE FIELD owner ON wallet TYPE record<account>;
DEFINE FIELD balance ON wallet TYPE int;
DEFINE FIELD frozen ON wallet TYPE bool;

DEFINE TABLE payment SCHEMAFULL;
DEFINE FIELD wallet ON payment TYPE record<wallet>;
DEFINE FIELD amount ON payment TYPE int;
DEFINE FIELD key ON payment TYPE string;
DEFINE INDEX payment_key ON payment FIELDS key UNIQUE;

DEFINE FUNCTION fn::membership_key($team: record<team>, $account: record<account>) -> string {
	RETURN string::concat(<string> $team, '/', <string> $account);
};
DEFINE FUNCTION fn::seats_taken($team: record<team>) -> int {
	RETURN count(SELECT id FROM membership WHERE team = $team);
};
DEFINE FUNCTION fn::payment_key($wallet: record<wallet>, $request: string) -> string {
	RETURN string::concat(<string> $wallet, '/', $request);
};
DEFINE FUNCTION fn::debit($balance: int, $amount: int) -> int {
	RETURN $balance - $amount;
};
