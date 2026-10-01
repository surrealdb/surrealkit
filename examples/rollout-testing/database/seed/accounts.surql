-- An account that exists before anyone signs up, for the suites to test
-- other people's records against.
UPSERT account:seeded SET email = 'seeded@example.test', passphrase = crypto::argon2::generate('not-a-real-passphrase');
