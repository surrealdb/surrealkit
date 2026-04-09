use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use super::types::RunReport;

pub fn print_human_report(report: &RunReport) {
	println!("Test run summary:");
	println!("  suites: {} total, {} failed", report.suites_total, report.suites_failed);
	println!(
		"  cases: {} total, {} passed, {} failed",
		report.cases_total, report.cases_passed, report.cases_failed
	);
	println!("  duration_ms: {}", report.duration_ms);

	for suite in &report.suites {
		println!(
			"suite {} [{} / {}]: {} passed, {} failed",
			suite.suite_name,
			suite.namespace,
			suite.database,
			suite.cases_passed,
			suite.cases_failed
		);
		for (index, case) in suite.cases.iter().enumerate() {
			if case.passed {
				continue;
			}
			println!("  FAIL case #{}", index + 1);
			for assertion in &case.assertions {
				if assertion.passed {
					continue;
				}
				println!("    - {} (failed)", assertion.name);
			}
		}
	}
}

pub fn write_json_report(path: &Path, report: &RunReport) -> Result<()> {
	if let Some(parent) = path.parent() {
		fs::create_dir_all(parent)
			.with_context(|| format!("creating report directory {}", parent.display()))?;
	}
	let raw = serde_json::to_string_pretty(report).context("serializing report json")?;
	fs::write(path, format!("{raw}\n"))
		.with_context(|| format!("writing report file {}", path.display()))?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use crate::tester::types::RunReport;

	#[test]
	fn json_report_is_serializable() {
		let report = RunReport {
			started_at: "2020-01-01T00:00:00Z".into(),
			finished_at: "2020-01-01T00:00:01Z".into(),
			duration_ms: 1000,
			suites_total: 1,
			suites_failed: 0,
			cases_total: 1,
			cases_passed: 1,
			cases_failed: 0,
			suites: Vec::new(),
		};
		let encoded = serde_json::to_string(&report).expect("serialization should work");
		assert!(encoded.contains("\"cases_total\":1"));
	}
}
