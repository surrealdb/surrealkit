-- Named queries: each becomes `<Name>Row` and `<Name>Params` types in the generated module.

-- name: ListCards
SELECT title, status, due, owner, tags FROM card WHERE status = $status ORDER BY title;

-- name: CardsByOwner
SELECT * FROM card WHERE owner = $owner;

-- name: CountByStatus
SELECT status, count() AS n FROM card GROUP BY status;
