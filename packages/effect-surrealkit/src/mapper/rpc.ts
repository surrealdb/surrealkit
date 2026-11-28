/**
 * JSON-RPC 2.0 with `Content-Length` framing, the transport TypeScript's
 * content mappers speak over the mapper's stdin and stdout. TypeScript sends
 * every request; the mapper only answers.
 */
import type { Readable, Writable } from 'node:stream';

export type RpcHandler = (params: unknown) => unknown;
export type RpcHandlers = Readonly<Record<string, RpcHandler>>;

interface Request {
	readonly id?: number | string | null;
	readonly method?: unknown;
	readonly params?: unknown;
}

/** One framed message. */
export const frame = (message: unknown): Buffer => {
	const body = Buffer.from(JSON.stringify(message), 'utf8');
	return Buffer.concat([
		Buffer.from(`Content-Length: ${body.length}\r\n\r\n`, 'ascii'),
		body,
	]);
};

/**
 * Reads framed messages from `input`, answers each request through
 * `handlers` on `output`, and resolves when `input` ends. A handler that
 * throws answers with error -32603, an unknown method with -32601, and a
 * notification (no `id`) gets no answer.
 */
export const serveRpc = (
	input: Readable,
	output: Writable,
	handlers: RpcHandlers,
): Promise<void> =>
	new Promise((done, fail) => {
		let buffer = Buffer.alloc(0);
		let pending = Promise.resolve();
		const answer = async (request: Request) => {
			const handler =
				typeof request.method === 'string'
					? handlers[request.method]
					: undefined;
			let reply: Record<string, unknown>;
			try {
				if (handler === undefined)
					reply = {
						error: {
							code: -32601,
							message: `unknown method ${String(request.method)}`,
						},
					};
				else reply = { result: (await handler(request.params)) ?? {} };
			} catch (cause) {
				reply = {
					error: {
						code: -32603,
						message:
							cause instanceof Error
								? cause.message
								: String(cause),
					},
				};
			}
			if (request.id === undefined) return;
			output.write(frame({ jsonrpc: '2.0', id: request.id, ...reply }));
		};
		input.on('data', (chunk: Buffer) => {
			buffer = Buffer.concat([buffer, chunk]);
			for (;;) {
				const headerEnd = buffer.indexOf('\r\n\r\n');
				if (headerEnd === -1) return;
				const header = buffer.subarray(0, headerEnd).toString('ascii');
				const length = /^content-length:\s*(\d+)\s*$/imu.exec(header);
				if (length === null) {
					fail(
						new Error(
							`a message without Content-Length: ${header}`,
						),
					);
					return;
				}
				const start = headerEnd + 4;
				const end = start + Number(length[1]);
				if (buffer.length < end) return;
				const body = buffer.subarray(start, end).toString('utf8');
				buffer = buffer.subarray(end);
				let request: Request;
				try {
					request = JSON.parse(body) as Request;
				} catch {
					output.write(
						frame({
							jsonrpc: '2.0',
							id: null,
							error: { code: -32700, message: 'parse error' },
						}),
					);
					continue;
				}
				// In order, so a transform never overtakes its openProject.
				pending = pending.then(() => answer(request));
			}
		});
		input.on('end', () => {
			pending.then(() => done(), fail);
		});
		input.on('error', fail);
	});
