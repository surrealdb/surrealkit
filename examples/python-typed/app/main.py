"""Typed queries: `TypedAsyncSurreal` and `TypedSurreal` know the result of each query text below.

`surrealkit compile` reads the literal `query(...)` and `live_query(...)` calls in this file and in
`runtime_check.py`, types them against `database/schema`, and writes `surreal_queries.py`.
"""

from __future__ import annotations

import asyncio
import sys

from surrealdb import AsyncSurreal, RecordID

from app.surreal_queries import TypedAsyncSurreal, TypedSurreal


async def open_cards(db: TypedAsyncSurreal) -> list[str]:
    """The cards still to do. `status` is `"todo" | "done"`, and `due` may be absent."""
    (cards,) = await db.query(
        "SELECT title, status, due, owner FROM card WHERE status = $status",
        {"status": "todo"},
    )
    lines: list[str] = []
    for card in cards:
        # `due` is `NotRequired`: `SELECT *` leaves a NONE field out of the row, and an explicit
        # projection like this one returns it as None, so read it with `.get`.
        due_at = card.get("due")
        due = due_at.date().isoformat() if due_at is not None else "no due date"
        lines.append(f"{card['title']} ({card['status']}, {due})")
    return lines


async def count_cards(db: TypedAsyncSurreal) -> int:
    """A query with no parameters takes no second argument."""
    (counts,) = await db.query("SELECT count() AS n FROM card GROUP ALL")
    return counts[0]["n"] if counts else 0


def cards_of(db: TypedSurreal, owner: RecordID) -> list[str]:
    """The blocking client has the same overloads."""
    (cards,) = db.query("SELECT title, tags FROM card WHERE owner = $owner", {"owner": owner})
    return [f"{card['title']}: {len(card['tags'])} tags" for card in cards]


async def watch_cards(db: TypedAsyncSurreal, owner: RecordID) -> None:
    """A live query yields typed notifications until it is killed."""
    async for notification in db.live_query(
        "SELECT * FROM card WHERE owner = $owner", {"owner": owner}
    ):
        if notification["action"] == "DELETE":
            print("removed", notification["record"])
        else:
            print(notification["action"], notification["result"]["title"])


async def main(url: str) -> None:
    connection = AsyncSurreal(url)
    await connection.connect()
    await connection.signin({"username": "root", "password": "root"})
    await connection.use("example", "example")
    db = TypedAsyncSurreal(connection)
    print(await open_cards(db), await count_cards(db))
    await connection.close()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "ws://127.0.0.1:8000/rpc"))
