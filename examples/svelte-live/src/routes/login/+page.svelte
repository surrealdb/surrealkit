<script lang="ts">
	let { form } = $props();
</script>

<main>
	<h1>Sign in</h1>
	<form method="POST" action="?/signin">
		<input name="email" type="email" placeholder="email" aria-label="email" required />
		<input name="pass" type="password" placeholder="password" aria-label="password" required />
		<button type="submit">Sign in</button>
		<button type="submit" formaction="?/signup">Sign up</button>
	</form>
	{#if form?.message}<p role="alert">{form.message}</p>{/if}
</main>
