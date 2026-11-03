use super::types::{FilterInput, LoadedSuite};

pub fn apply_filters(mut suites: Vec<LoadedSuite>, filters: &FilterInput) -> Vec<LoadedSuite> {
	suites.retain(|suite| match_suite(suite, filters.suite_pattern.as_deref().unwrap_or("*")));
	// With --exec, a suite that never had cases exists to run the command; one whose cases
	// were all filtered out was not selected.
	let exec_only: Vec<bool> =
		suites.iter().map(|suite| filters.exec && suite.spec.cases.is_empty()).collect();

	for suite in &mut suites {
		suite.spec.cases.retain(|case| {
			match_case(case.name.as_str(), filters.case_pattern.as_deref().unwrap_or("*"))
		});

		if !filters.tags.is_empty() {
			let suite_tags = suite.spec.tags.clone();
			suite.spec.cases.retain(|case| {
				filters.tags.iter().all(|tag| {
					suite_tags.iter().any(|x| x == tag) || case.tags.iter().any(|x| x == tag)
				})
			});
		}
	}

	let mut exec_only = exec_only.into_iter();
	suites.retain(|suite| exec_only.next().unwrap_or(false) || !suite.spec.cases.is_empty());
	suites
}

fn match_suite(suite: &LoadedSuite, pattern: &str) -> bool {
	let suite_name =
		suite.spec.name.clone().unwrap_or_else(|| suite.path.to_string_lossy().to_string());
	let suite_path = suite.path.to_string_lossy().to_string();
	glob_match(pattern, &suite_name) || glob_match(pattern, &suite_path)
}

fn match_case(name: &str, pattern: &str) -> bool {
	glob_match(pattern, name)
}

pub fn glob_match(pattern: &str, text: &str) -> bool {
	let p: Vec<char> = pattern.chars().collect();
	let t: Vec<char> = text.chars().collect();
	let mut dp = vec![vec![false; t.len() + 1]; p.len() + 1];
	dp[0][0] = true;

	for i in 1..=p.len() {
		if p[i - 1] == '*' {
			dp[i][0] = dp[i - 1][0];
		}
	}

	for i in 1..=p.len() {
		for j in 1..=t.len() {
			if p[i - 1] == '*' {
				dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
			} else if p[i - 1] == '?' || p[i - 1] == t[j - 1] {
				dp[i][j] = dp[i - 1][j - 1];
			}
		}
	}

	dp[p.len()][t.len()]
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use super::{apply_filters, glob_match};
	use crate::tester::types::{FilterInput, LoadedSuite};

	fn suite(name: &str, raw: &str) -> LoadedSuite {
		LoadedSuite {
			path: PathBuf::from(format!("{name}.toml")),
			spec: toml::from_str(&format!("name = \"{name}\"\n{raw}")).expect("suite"),
		}
	}

	#[test]
	fn with_exec_a_suite_without_cases_still_runs_but_a_filtered_out_one_does_not() {
		let case = "[[cases]]\nname = \"c\"\nkind = \"sql_expect\"\nsql = \"RETURN 1;\"\n";
		let suites = || vec![suite("bare", ""), suite("full", case)];
		let names = |kept: Vec<LoadedSuite>| -> Vec<String> {
			kept.into_iter().map(|s: LoadedSuite| s.spec.name.unwrap_or_default()).collect()
		};
		let mut filters = FilterInput {
			suite_pattern: None,
			case_pattern: None,
			tags: Vec::new(),
			exec: false,
		};
		assert_eq!(names(apply_filters(suites(), &filters)), ["full"]);
		filters.exec = true;
		assert_eq!(names(apply_filters(suites(), &filters)), ["bare", "full"]);
		filters.case_pattern = Some("nothing".into());
		assert_eq!(names(apply_filters(suites(), &filters)), ["bare"]);
	}

	#[test]
	fn glob_match_handles_wildcards() {
		assert!(glob_match("*", "abc"));
		assert!(glob_match("a*", "abc"));
		assert!(glob_match("a?c", "abc"));
		assert!(!glob_match("a?d", "abc"));
	}
}
