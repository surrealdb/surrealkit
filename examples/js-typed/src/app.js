// @ts-check
import { Surreal } from "surrealdb";

/** @typedef {import("./queries").Typed<Surreal>} TypedSurreal */

/** A client whose `query()` knows the result of each query text in this project. */
export const db = /** @type {TypedSurreal} */ (new Surreal());

/** @param {"member" | "admin"} role */
export async function listPeople(role) {
	const [people] = await db.query("SELECT name, email, age FROM person WHERE role = $role", {
		role,
	});
	return people.map((person) => `${person.name} <${person.email}> ${person.age ?? "?"}`);
}

export async function countPeople() {
	const [counts] = await db.query("SELECT count() AS n FROM person GROUP ALL");
	return counts[0]?.n ?? 0;
}
