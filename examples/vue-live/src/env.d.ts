/// <reference types="vite/client" />

interface ImportMetaEnv {
	/** The database the browser connects to. */
	readonly VITE_SURREAL_URL?: string;
}
