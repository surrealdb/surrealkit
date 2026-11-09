'use client';

import { useRef } from 'react';
import { LiveQuery } from 'react-surrealkit';

import { createTask } from '@/app/actions';

/** A form that posts to a server action; the new task arrives through the live list. */
export function NewTask() {
	const form = useRef<HTMLFormElement>(null);
	return (
		<form
			ref={form}
			className="new"
			action={async (data) => {
				await createTask(data);
				form.current?.reset();
			}}
		>
			<input name="title" placeholder="A new task" aria-label="Title" required />
			<select name="owner" aria-label="Owner" defaultValue="">
				<option value="">nobody</option>
				<LiveQuery q="SELECT id, name FROM person ORDER BY name">
					{(people) =>
						people.map((person) => (
							<option key={String(person.id)} value={String(person.id.id)}>
								{person.name}
							</option>
						))
					}
				</LiveQuery>
			</select>
			<button type="submit">Add</button>
		</form>
	);
}
