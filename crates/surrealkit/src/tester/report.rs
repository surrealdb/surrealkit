use std::path::Path;
use std::{fs, io::Write};

use anyhow::{Context, Result};

use super::types::RunReport;

pub fn print_human_report<W: Write>(out: &mut W, report: &RunReport) -> Result<()> {
	let status = if report.suites_failed > 0 || report.cases_failed > 0 {
		"FAIL"
	} else {
		"PASS"
	};
	writeln!(out, "Test run: {status} ({}ms)", report.duration_ms)?;
	writeln!(out, "")?;

	writeln!(out, "Summary")?;
	writeln!(out, "  suites: {} total, {} failed", report.suites_total, report.suites_failed)?;
	writeln!(
		out,
		"  cases : {} total, {} passed, {} failed",
		report.cases_total, report.cases_passed, report.cases_failed
	)?;

	if report.suites_failed > 0 {
		writeln!(out, "")?;
		writeln!(out, "Failures ({})", report.suites_failed)?;
		for suite in &report.suites {
			if suite.cases_failed == 0 {
				continue;
			}

			writeln!(out, "  suite `{}`", suite.suite_name,)?;
			writeln!(out, "    file     : {}", suite.suite_file)?;
			writeln!(out, "    namespace: {}", suite.namespace)?;
			writeln!(out, "    database : {}", suite.database)?;
			writeln!(
				out,
				"    result   : {} passed, {} failed",
				suite.cases_passed, suite.cases_failed
			)?;
			for (index, case) in suite.cases.iter().enumerate() {
				if case.passed {
					continue;
				}
				writeln!(out, "    case `{}` (#{})", case.name, index + 1)?;
				for assertion in &case.assertions {
					if assertion.passed {
						continue;
					}
					writeln!(out, "      - `{}` (failed): {}", assertion.name, assertion.message)?;
				}
			}
		}
	}

	Ok(())
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
	use super::*;
	use crate::tester::types::{AssertionReport, CaseReport, RunReport, SuiteReport};

	#[test]
	fn human_report_prints_summary() -> Result<()> {
		let report = make_report_successful();
		let mut output = Vec::new();
		print_human_report(&mut output, &report)?;
		let text = String::from_utf8(output)?;
		insta::assert_snapshot!(text, @"
		Test run: PASS (1000ms)

		Summary
		  suites: 1 total, 0 failed
		  cases : 1 total, 1 passed, 0 failed
		");
		Ok(())
	}

	#[test]
	fn human_report_prints_failure() -> Result<()> {
		let report = make_report_failure();
		let mut output = Vec::new();
		print_human_report(&mut output, &report)?;
		let text = String::from_utf8(output)?;
		insta::assert_snapshot!(text, @"
		Test run: FAIL (1000ms)

		Summary
		  suites: 1 total, 1 failed
		  cases : 1 total, 0 passed, 1 failed

		Failures (1)
		  suite `test_suite_name`
		    file     : test_suite_file.yaml
		    namespace: test_ns
		    database : test_db
		    result   : 0 passed, 1 failed
		    case `test_case_name` (#1)
		      - `test_assertion_name` (failed): assertion failed for some reasons
		");
		Ok(())
	}

	#[test]
	fn json_report_is_serializable() {
		let report = make_report_successful();
		let encoded = serde_json::to_string(&report).expect("serialization should work");
		let decoded: serde_json::Value =
			serde_json::from_str(&encoded).expect("deserialization should work");

		insta::assert_json_snapshot!(
			decoded,
			@r#"
		{
		  "cases_failed": 0,
		  "cases_passed": 1,
		  "cases_total": 1,
		  "duration_ms": 1000,
		  "finished_at": "2020-01-01T00:00:01Z",
		  "started_at": "2020-01-01T00:00:00Z",
		  "suites": [
		    {
		      "cases": [
		        {
		          "assertions": [
		            {
		              "message": "assertion passed",
		              "name": "test_assertion_name",
		              "passed": true
		            }
		          ],
		          "duration_ms": 1000,
		          "kind": "sql_expect",
		          "message": null,
		          "name": "test_case_name",
		          "passed": true
		        }
		      ],
		      "cases_failed": 0,
		      "cases_passed": 1,
		      "cases_total": 1,
		      "database": "test_db",
		      "duration_ms": 1000,
		      "namespace": "test_ns",
		      "suite_file": "test_suite_file.yaml",
		      "suite_name": "test_suite_name"
		    }
		  ],
		  "suites_failed": 0,
		  "suites_total": 1
		}
		"#
		);
	}

	// ---------

	fn make_report_successful() -> RunReport {
		make_report(vec![CaseReport {
			name: "test_case_name".into(),
			passed: true,
			kind: "sql_expect".into(),
			duration_ms: 1000,
			message: None,
			assertions: vec![AssertionReport {
				name: "test_assertion_name".into(),
				passed: true,
				message: "assertion passed".into(),
			}],
		}])
	}

	fn make_report_failure() -> RunReport {
		make_report(vec![CaseReport {
			name: "test_case_name".into(),
			passed: false,
			kind: "sql_expect".into(),
			duration_ms: 1000,
			message: None,
			assertions: vec![AssertionReport {
				name: "test_assertion_name".into(),
				passed: false,
				message: "assertion failed for some reasons".into(),
			}],
		}])
	}

	fn make_report(cases: Vec<CaseReport>) -> RunReport {
		let cases_total = cases.len();
		let cases_passed = cases.iter().filter(|c| c.passed).count();
		let cases_failed = cases.iter().filter(|c| !c.passed).count();
		let suites_failed = if cases_failed > 0 {
			1
		} else {
			0
		};
		RunReport {
			started_at: "2020-01-01T00:00:00Z".into(),
			finished_at: "2020-01-01T00:00:01Z".into(),
			duration_ms: 1000,
			suites_total: 1,
			suites_failed,
			cases_total,
			cases_passed,
			cases_failed,
			suites: vec![SuiteReport {
				suite_file: "test_suite_file.yaml".into(),
				suite_name: "test_suite_name".into(),
				namespace: "test_ns".into(),
				database: "test_db".into(),
				duration_ms: 1000,
				cases_total,
				cases_passed,
				cases_failed,
				cases,
			}],
		}
	}
}
