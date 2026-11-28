import { Surreal, type SurrealSession } from 'surrealdb';

import { type Connection, layerFrom } from '../src/testing/index.js';
import type { Engine } from './engine.js';

/** A fresh unauthenticated session, ready for the fixture's record signup. */
export const openRecordSession = async (
	engine: Engine,
): Promise<SurrealSession> => {
	if (engine.transport !== 'http') return engine.db.newSession();
	if (engine.rpcUrl === undefined) {
		throw new Error('The HTTP test engine has no RPC endpoint');
	}
	// HTTP lacks SDK child sessions. Open a separate connection with no root
	// credentials, leaving the engine's admin connection untouched.
	const connection = new Surreal();
	try {
		await connection.connect(engine.rpcUrl);
		return connection;
	} catch (error) {
		await connection.close();
		throw error;
	}
};

/** Keep the database layer's admin queries and give recordUser an isolated session factory. */
export const recordUserLayer = (engine: Engine) => {
	const { db, namespace } = engine;
	const connection: Connection =
		engine.transport === 'http'
			? {
					query: db.query.bind(db),
					transaction: db.transaction.bind(db),
					newSession: () => openRecordSession(engine),
				}
			: db;
	return layerFrom(connection, { namespace, database: namespace });
};
