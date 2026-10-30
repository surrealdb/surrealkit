/** The `effect-surrealkit` executable. */
import { run } from './run.js';

const readStdin = async (): Promise<string> => {
	const chunks: Buffer[] = [];
	for await (const chunk of process.stdin) chunks.push(chunk as Buffer);
	return Buffer.concat(chunks).toString('utf8');
};

process.exitCode = await run(process.argv.slice(2), {
	cwd: process.cwd(),
	stdin: readStdin,
	stdout: (text) => process.stdout.write(text),
	stderr: (text) => process.stderr.write(text),
	env: process.env,
});
