import { Board } from '@/components/Board';
import { loadTasks } from '@/lib/tasks.server';

// Rendered per request: the first paint is the database as it is now.
export const dynamic = 'force-dynamic';

export default async function Page() {
	const tasks = await loadTasks();
	return (
		<main>
			<h1>Tasks</h1>
			<Board tasks={tasks} />
		</main>
	);
}
