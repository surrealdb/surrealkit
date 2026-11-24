/** Types of `skips.mjs`, for the tests that import it. */
export interface VitestReport {
	readonly testResults: ReadonlyArray<{
		readonly name: string;
		readonly assertionResults: ReadonlyArray<{
			readonly ancestorTitles: ReadonlyArray<string>;
			readonly title: string;
			readonly status: string;
		}>;
	}>;
}

export declare const laneOf: (
	env: Readonly<Record<string, string | undefined>>,
	binaryVersion?: () => string,
	serverVersion?: () => string,
) => string;

export declare const versionUrl: (rpc: string) => string;

export declare const pinnedLanes: (root?: string) => ReadonlyArray<string>;

export declare const skippedTests: (
	report: VitestReport,
	root: string,
) => ReadonlyArray<string>;

export declare const parseList: (text: string) => ReadonlyArray<string>;

export declare const renderList: (
	lane: string,
	names: ReadonlyArray<string>,
) => string;

export declare const compareSkips: (
	expected: ReadonlyArray<string>,
	actual: ReadonlyArray<string>,
) => {
	readonly added: ReadonlyArray<string>;
	readonly removed: ReadonlyArray<string>;
};
