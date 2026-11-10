// Type goldens for the composables. Each `@ts-expect-error` is a case the
// compiler must reject.

import type { RecordId } from 'surrealdb';
import { computed, reactive, ref } from 'vue';

import { useLive, useQuery } from '../../src/index.js';
import './register.js';

const BY_NAME = 'SELECT name, email FROM user WHERE name = $name LIMIT $limit';

export function typed() {
	const name = ref('ada');

	// Params as a getter, a ref, a computed, a reactive object, or a value.
	useQuery(BY_NAME, () => ({ name: name.value }));
	useQuery(BY_NAME, ref({ name: 'ada' }));
	useQuery(
		BY_NAME,
		computed(() => ({ name: name.value, limit: 3 })),
	);
	useQuery(BY_NAME, reactive({ name: 'ada', limit: 3 }));
	const once = useQuery(BY_NAME, { name: 'ada' });
	once.data.value satisfies
		| Array<{ email: string; name: string }>
		| undefined;

	const events = useLive('SELECT * FROM event');
	events.data.value satisfies Array<{ id: RecordId<'event'>; kind: string }>;
	const kinds = useLive('SELECT kind FROM event', undefined, {
		select: (rows) => rows.map((row) => row.kind),
	});
	kinds.selected.value satisfies string[];

	// A getter for q, as for params.
	useLive(() => 'SELECT kind FROM event' as const);

	// @ts-expect-error a required parameter is missing
	useQuery(BY_NAME);
	// @ts-expect-error a parameter has the wrong type
	useQuery(BY_NAME, { name: 1 });
	// @ts-expect-error a getter returning the wrong parameters (a function has a `name`)
	useQuery(BY_NAME, () => ({ name: 1 }));
	// @ts-expect-error FROM ONLY is not a row stream
	useLive('SELECT * FROM ONLY user:alice');
	// @ts-expect-error a query with no parameters takes none
	useLive('SELECT kind FROM event', { stray: 1 });
}
