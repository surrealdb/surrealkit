/**
 * The process TypeScript starts for the content mapper (`package.json`,
 * `typescript.contentMapper.exec`). It ends when TypeScript closes its
 * stdin.
 */

import { serveRpc } from './rpc.js';
import { mapperHandlers } from './server.js';

serveRpc(process.stdin, process.stdout, mapperHandlers()).then(
	() => process.exit(0),
	(error: unknown) => {
		process.stderr.write(
			`effect-surrealkit content mapper: ${error instanceof Error ? error.message : String(error)}\n`,
		);
		process.exit(1);
	},
);
