'use client';

import { useTransition } from 'react';
import { LiveQuery, useLive } from 'react-surrealkit';

import { deleteTask, moveTask } from '@/app/actions';
import type { TasksPayload } from '@/lib/tasks.server';
import { NewTask } from './NewTask';
import { Search } from './Search';

const STATUSES = ['todo', 'doing', 'done'] as const;

/**
 * Every task, live. The first render is the server's rows (the payload);
 * the hook then subscribes in the browser, and every change, from this tab,
 * another tab or a server action, streams in.
 */
export function Board({ tasks }: { tasks: TasksPayload }) {
	const all = useLive(tasks);
	const done = all.data.filter((task) => task.status === 'done').length;
	const [pending, start] = useTransition();

	return (
		<>
			<p className="muted" data-testid="summary">
				{all.data.length} tasks · {done} done
			</p>
			<NewTask />
			<LiveQuery
				q={all}
				loading={<p className="muted">Loading…</p>}
				empty={<p className="muted">No tasks yet. Add one above.</p>}
				error={(failure) => (
					<p role="alert" className="error">
						Can't reach the database: {failure.message}
					</p>
				)}
			>
				{(rows, { stale }) => (
					<ul className={stale || pending ? 'tasks dim' : 'tasks'}>
						{rows.map((task) => {
							const id = String(task.id.id);
							return (
								<li key={id} data-status={task.status}>
									<span className="title">{task.title}</span>
									<span className="muted">{task.owner ?? 'nobody'}</span>
									<select
										aria-label={`Status of ${task.title}`}
										value={task.status}
										onChange={(event) => {
											const status = event.currentTarget.value as (typeof STATUSES)[number];
											start(() => moveTask(id, status));
										}}
									>
										{STATUSES.map((status) => (
											<option key={status}>{status}</option>
										))}
									</select>
									<button type="button" onClick={() => start(() => deleteTask(id))}>
										Delete
									</button>
								</li>
							);
						})}
					</ul>
				)}
			</LiveQuery>
			<Search />
		</>
	);
}
