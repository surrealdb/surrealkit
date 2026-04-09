use super::types::{FilterInput, LoadedSuite};

pub fn apply_filters(mut suites: Vec<LoadedSuite>, filters: &FilterInput) -> Vec<LoadedSuite> {
	suites.retain(|suite| match_suite(suite, filters.suite_pattern.as_deref().unwrap_or("*")));

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

	suites.retain(|suite| !suite.spec.cases.is_empty());
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
	use super::glob_match;

	#[test]
	fn glob_match_handles_wildcards() {
		assert!(glob_match("*", "abc"));
		assert!(glob_match("a*", "abc"));
		assert!(glob_match("a?c", "abc"));
		assert!(!glob_match("a?d", "abc"));
	}
}
