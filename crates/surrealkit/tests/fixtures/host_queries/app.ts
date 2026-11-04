import Surreal from 'surrealdb';

const db = new Surreal();

export async function load(min: number) {
	const [people] = await db.query("SELECT name FROM person");
	const [adults] = await db.query('SELECT * FROM person WHERE age >= $age', { age: 18 });
	const [named] = await db.query(`SELECT * FROM person WHERE age > ${min}`);
	return { people, adults, named };
}
