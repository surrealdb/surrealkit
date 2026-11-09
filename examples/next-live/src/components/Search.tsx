'use client';

import { useState } from 'react';
import { Query } from 'react-surrealkit';

type Filter = 'all' | 'todo' | 'doing' | 'done';

/**
 * A one-shot search. The text is a query parameter, so typing runs the query
 * again (the previous results stay, dimmed, until the new ones land); the
 * status filter is applied by `select`, without a query. Nothing runs until
 * there is text to search for.
 */
export function Search() {
	const [text, setText] = useState('');
	const [filter, setFilter] = useState<Filter>('all');
	return (
		<section className="search">
			<h2>Search</h2>
			<input
				value={text}
				onChange={(event) => setText(event.currentTarget.value)}
				placeholder="Search titles"
				aria-label="Search"
			/>
			<select
				value={filter}
				onChange={(event) => setFilter(event.currentTarget.value as Filter)}
				aria-label="Filter by status"
			>
				<option value="all">any status</option>
				<option value="todo">todo</option>
				<option value="doing">doing</option>
				<option value="done">done</option>
			</select>
			<Query
				q="SELECT id, title, status, ->blocks->task.title AS blocks FROM task WHERE string::contains(string::lowercase(title), $text) ORDER BY title"
				params={{ text: text.trim().toLowerCase() }}
				enabled={text.trim().length > 0}
				select={(rows) => rows.filter((row) => filter === 'all' || row.status === filter)}
				loading={<p className="muted">Searching…</p>}
				empty={<p className="muted">No match.</p>}
			>
				{(rows, { stale }) => (
					<ul className={stale ? 'results dim' : 'results'} data-testid="results">
						{rows.map((row) => (
							<li key={String(row.id)}>
								{row.title} <span className="muted">({row.status})</span>
								{row.blocks.length > 0 && (
									<span className="muted"> · blocks {row.blocks.join(', ')}</span>
								)}
							</li>
						))}
					</ul>
				)}
			</Query>
		</section>
	);
}
