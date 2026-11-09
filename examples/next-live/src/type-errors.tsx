// Not imported by the app: `npm run typecheck` (tsc) compiles it, and each
// `@ts-expect-error` must meet a type error, or the check fails. Together
// they show the generated types reach every form of the API.

import { LiveQuery, Query, useLive, useQuery } from 'react-surrealkit';

const ALL =
	'SELECT id, title, status, owner.name AS owner, created FROM task ORDER BY created DESC';

export function TypeErrors() {
	const all = useLive(ALL);
	// A row's status is the schema's literal union.
	all.data.map((task) => task.status satisfies 'todo' | 'doing' | 'done');
	// @ts-expect-error `title` is a string, not a number.
	all.data.map((task) => task.title satisfies number);
	// @ts-expect-error a required parameter (`$id`) is missing.
	useQuery('UPDATE $id SET status = $status');
	return (
		<>
			{/* @ts-expect-error a hook result carries its own params; none may be passed beside it. */}
			<LiveQuery q={all} params={{ status: 'todo' }} />
			<Query
				q="SELECT id, title, status, ->blocks->task.title AS blocks FROM task WHERE string::contains(string::lowercase(title), $text) ORDER BY title"
				// @ts-expect-error `$text` is a string.
				params={{ text: 1 }}
			/>
		</>
	);
}
