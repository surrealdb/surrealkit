"""Runs the typed clients against a server and checks what comes back at runtime.

The types are only as true as the SDK's decoding, so this checks both: the shape of each row, the
types of its fields, and the notifications of a live query. It exits non-zero on the first run with
any failure.

    SURREAL_URL=ws://127.0.0.1:8768/rpc python -m app.runtime_check

The server must have root credentials root/root. Setup that is not a typed query goes through
variables, so `surrealkit compile` (which reads literal queries) does not type it.
"""

from __future__ import annotations

import asyncio
import datetime
import os
import sys
from pathlib import Path

from surrealdb import AsyncSurreal, RecordID, Surreal

from app.surreal_queries import TypedAsyncSurreal, TypedSurreal

URL = os.environ.get("SURREAL_URL", "ws://127.0.0.1:8000/rpc")
SCHEMA = (Path(__file__).parent.parent / "database/schema/001_cards.surql").read_text()

# A fresh server has no namespace or database to `USE`.
DEFINE = "DEFINE NAMESPACE IF NOT EXISTS example; USE NS example; DEFINE DATABASE IF NOT EXISTS example;"
SEED = """
CREATE person:ada SET name = 'Ada';
CREATE person:bob SET name = 'Bob';
CREATE card:docs SET title = 'Write docs', status = 'todo', due = d'2026-12-01T09:30:00Z', owner = person:ada, tags = ['docs', 'writing'];
CREATE card:ship SET title = 'Ship', status = 'done', owner = person:ada, tags = [];
CREATE card:review SET title = 'Review', status = 'todo', owner = person:bob, tags = ['review'];
"""
PROBE = "RETURN 1"
CREATE = "CREATE card:live SET title = 'Live', status = 'todo', owner = person:ada, tags = []"
UPDATE = "UPDATE card:live SET status = 'done'"
DELETE = "DELETE card:live"

failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(("ok   " if condition else "FAIL ") + message)
    if not condition:
        failures.append(message)


def kind(value: object) -> str:
    return type(value).__name__


async def check_async(db: TypedAsyncSurreal) -> None:
    (cards,) = await db.query(
        "SELECT title, status, due, owner FROM card WHERE status = $status",
        {"status": "todo"},
    )
    check(kind(cards) == "list" and len(cards) == 2, "async: two todo cards, as a list")
    by_title = {card["title"]: card for card in cards}
    check(set(by_title) == {"Write docs", "Review"}, "async: titles are str")
    docs, review = by_title["Write docs"], by_title["Review"]
    check(set(docs) == {"title", "status", "due", "owner"}, "async: a projection has its keys")
    check(review.get("due") is None, "async: an unset option<datetime> reads as None")
    check(isinstance(docs.get("due"), datetime.datetime), "async: datetime is datetime.datetime")
    check(kind(docs["owner"]) == "RecordID", "async: record<person> is RecordID")
    check(docs["status"] in ("todo", "done"), "async: status is one of its literals")

    (counts,) = await db.query("SELECT count() AS n FROM card GROUP ALL")
    check(counts == [{"n": 3}] and kind(counts[0]["n"]) == "int", "async: count() is an int")

    (tagged,) = await db.query(
        "SELECT title, tags FROM card WHERE owner = $owner",
        {"owner": RecordID("person", "ada")},
    )
    check(
        all(kind(card["tags"]) == "SurrealSet" for card in tagged) and len(tagged) == 2,
        "async: set<string> is SurrealSet",
    )

    # `SELECT *` leaves an unset option<datetime> out of the row, which is what `NotRequired` says.
    (rows,) = await db.query(
        "SELECT * FROM card WHERE owner = $owner", {"owner": RecordID("person", "ada")}
    )
    by_id = {row["id"].id: row for row in rows}
    check(set(by_id) == {"docs", "ship"}, "async: SELECT * returns the owner's cards")
    check("due" in by_id["docs"] and "due" not in by_id["ship"], "async: SELECT * omits an unset option")
    check(kind(by_id["docs"]["tags"]) == "SurrealSet", "async: SELECT * tags are a SurrealSet")

    # The raw connection is the way to send a text the module does not know.
    other = await db.raw.query(PROBE)
    check(other == [1], "async: .raw.query returns the SDK's list of results")


def check_blocking(db: TypedSurreal) -> None:
    (cards,) = db.query(
        "SELECT title, status, due, owner FROM card WHERE status = $status",
        {"status": "done"},
    )
    check(
        len(cards) == 1 and cards[0]["title"] == "Ship" and cards[0].get("due") is None,
        "blocking: the done card, with no due",
    )
    (tagged,) = db.query(
        "SELECT title, tags FROM card WHERE owner = $owner", {"owner": RecordID("person", "bob")}
    )
    check(len(tagged) == 1 and list(tagged[0]["tags"]) == ["review"], "blocking: tags decode")


async def check_live(db: TypedAsyncSurreal) -> None:
    notifications = db.live_query(
        "SELECT * FROM card WHERE owner = $owner", {"owner": RecordID("person", "ada")}
    )
    seen: list[str] = []
    results: list[dict[str, object]] = []

    async def consume() -> None:
        async for notification in notifications:
            seen.append(notification["action"])
            check(kind(notification["id"]) == "UUID", f"live {notification['action']}: id is a UUID")
            check(
                kind(notification["session"]) == "UUID",
                f"live {notification['action']}: session is a UUID",
            )
            check(
                notification["record"] == RecordID("card", "live"),
                f"live {notification['action']}: record is card:live",
            )
            result = notification["result"]
            check(result["title"] == "Live", f"live {notification['action']}: result.title")
            results.append(dict(result))
            if len(seen) == 3:
                return

    task = asyncio.create_task(consume())
    await asyncio.sleep(1.0)  # the subscription starts when the generator first runs
    await db.raw.query(CREATE)
    await db.raw.query(UPDATE)
    await db.raw.query(DELETE)
    try:
        await asyncio.wait_for(task, timeout=10)
    except TimeoutError:
        check(False, f"live: three notifications within 10s (saw {seen})")
        return
    check(seen == ["CREATE", "UPDATE", "DELETE"], f"live: CREATE, UPDATE, DELETE in order ({seen})")
    check(results[1]["status"] == "done", "live: UPDATE carries the new status")


async def run() -> None:
    async with AsyncSurreal(URL) as connection:
        await connection.signin({"username": "root", "password": "root"})
        await connection.query(DEFINE)
        await connection.use("example", "example")
        await connection.query(SCHEMA)
        await connection.query(SEED)
        typed = TypedAsyncSurreal(connection)
        await check_async(typed)
        await check_live(typed)
    with Surreal(URL) as blocking:
        blocking.signin({"username": "root", "password": "root"})
        blocking.use("example", "example")
        check_blocking(TypedSurreal(blocking))


if __name__ == "__main__":
    asyncio.run(run())
    if failures:
        print(f"{len(failures)} check(s) failed")
        sys.exit(1)
    print("all checks passed")
