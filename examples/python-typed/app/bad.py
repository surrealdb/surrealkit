"""Mistakes the generated types reject. Both checkers must report each marked line, and nothing else.

Each line carries mypy's `# type: ignore[code]` and pyright's `# pyright: ignore[rule]`; with
`warn_unused_ignores` and `reportUnnecessaryTypeIgnoreComment` an ignore that hides nothing is
itself an error, so removing a mistake from this file fails the check.
"""

from __future__ import annotations

from app.surreal_queries import TypedAsyncSurreal


async def mistakes(db: TypedAsyncSurreal) -> None:
    # A parameter of the wrong type.
    await db.query("SELECT title, status, due, owner FROM card WHERE status = $status", {"status": 3})  # type: ignore[call-overload]  # pyright: ignore[reportArgumentType, reportCallIssue]
    # A literal the column does not allow.
    await db.query("SELECT title, status, due, owner FROM card WHERE status = $status", {"status": "doing"})  # type: ignore[call-overload]  # pyright: ignore[reportArgumentType, reportCallIssue]
    # A query text the module does not know: it has no overload. Send it through `db.raw`.
    await db.query("SELECT * FROM nowhere")  # type: ignore[call-overload]  # pyright: ignore[reportCallIssue, reportArgumentType]
    (cards,) = await db.query(
        "SELECT title, status, due, owner FROM card WHERE status = $status", {"status": "todo"}
    )
    for card in cards:
        # A field the query does not select.
        print(card["titel"])  # type: ignore[typeddict-item]  # pyright: ignore[reportGeneralTypeIssues, reportUnknownArgumentType]
