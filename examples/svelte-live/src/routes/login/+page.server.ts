import { dev } from '$app/environment';
import { fail, redirect } from '@sveltejs/kit';
import { Surreal } from 'surrealdb';

import { ACCESS, SCOPE, SURREAL_URL } from '$lib/db';

import type { Actions } from './$types';

/** Signs in (or up) with record access, and keeps the token in a cookie. */
async function enter(mode: 'signin' | 'signup', form: FormData) {
	const email = String(form.get('email') ?? '');
	const pass = String(form.get('pass') ?? '');
	const db = new Surreal();
	try {
		await db.connect(SURREAL_URL, SCOPE);
		const auth = { ...SCOPE, access: ACCESS, variables: { email, pass } };
		const tokens = mode === 'signup' ? await db.signup(auth) : await db.signin(auth);
		return tokens.access;
	} finally {
		await db.close();
	}
}

function action(mode: 'signin' | 'signup'): Actions[string] {
	return async ({ request, cookies }) => {
		let token: string;
		try {
			token = await enter(mode, await request.formData());
		} catch {
			return fail(400, { mode, message: mode === 'signup' ? 'Could not sign up.' : 'Wrong email or password.' });
		}
		cookies.set('token', token, {
			path: '/',
			httpOnly: true,
			sameSite: 'lax',
			secure: !dev,
			maxAge: 60 * 60,
		});
		redirect(303, '/');
	};
}

export const actions: Actions = { signin: action('signin'), signup: action('signup') };
