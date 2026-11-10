// @ts-check
// Mistakes the generated types reject: `npm run typecheck` fails on this file and only on it.
import { db } from "./app.js";

export async function mistakes() {
	const [people] = await db.query("SELECT name, email, age FROM person WHERE role = $role", {
		role: "admin",
	});
	// A field the query does not select.
	return people.map((person) => person.nick);
}
