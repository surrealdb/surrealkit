use std::path::Path;
use std::{fs, io::Write};

use super::types::RunReport;
use anyhow::{Context, Result};
use indoc::writedoc;

fn indent_block(s: &str) -> String {
	s.lines().map(|line| format!("  {line}")).collect::<Vec<_>>().join("\n")
}

pub fn print_human_report<W: Write>(out: &mut W, report: &RunReport) -> Result<()> {
	let has_failure = report.suites_failed > 0 || report.cases_failed > 0;
	writedoc!(
		out,
		"
		Test run: {status} ({duration_ms}ms)

		Summary
		- suites: {suites_total} total, {suites_failed} failed
		- cases : {cases_total} total, {cases_passed} passed, {cases_failed} failed
		",
		status = if has_failure {
			"FAIL"
		} else {
			"PASS"
		},
		duration_ms = report.duration_ms,
		suites_total = report.suites_total,
		suites_failed = report.suites_failed,
		cases_total = report.cases_total,
		cases_passed = report.cases_passed,
		cases_failed = report.cases_failed
	)?;

	if report.suites_failed > 0 {
		for suite in &report.suites {
			if suite.cases_failed == 0 {
				continue;
			}
			writedoc!(
				out,
				"

				Suite: {suite_name}
				- file     : {suite_file}
				- namespace: {namespace}
				- database : {database}
				- result   : {cases_passed} passed, {cases_failed} failed
				",
				suite_name = suite.suite_name,
				suite_file = suite.suite_file,
				namespace = suite.namespace,
				database = suite.database,
				cases_passed = suite.cases_passed,
				cases_failed = suite.cases_failed
			)?;

			for (index, case) in suite.cases.iter().enumerate() {
				if case.passed {
					continue;
				}
				writedoc!(
					out,
					"

					Case {n}: {case_name}
					",
					n = index + 1,
					case_name = case.name
				)?;
				if let Some(message) = &case.message {
					writeln!(out, "\n{}\n", message.trim())?;
				}

				for assertion in &case.assertions {
					if assertion.passed {
						continue;
					}
					let header = format!("- {}", assertion.name);
					let line_count = assertion.message.lines().count();
					match line_count {
						0 => writeln!(out, "{}", header)?,
						1 => writeln!(out, "{}: {}", header, assertion.message)?,
						_ => {
							writeln!(out, "{}:", header)?;
							writeln!(out, "{}", indent_block(&assertion.message))?;
						}
					}
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
		- suites: 1 total, 0 failed
		- cases : 1 total, 1 passed, 0 failed
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
		- suites: 1 total, 1 failed
		- cases : 1 total, 0 passed, 1 failed

		Suite: test_suite_name
		- file     : test_suite_file.yaml
		- namespace: test_ns
		- database : test_db
		- result   : 0 passed, 1 failed

		Case 1: test_case_name

		Case mutli-line
		message with some details

		- test_no_message_assertion
		- test_short_assertion: assertion failed for some reasons
		- test_multi_line_assertion:
		  multi line assertion failed for some reasons
		  second line
		    third line
		  
		  fourth line
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
			message: Some(
				indoc::indoc!(
					"
				Case mutli-line
				message with some details
				"
				)
				.into(),
			),
			assertions: vec![
				AssertionReport {
					name: "test_no_message_assertion".into(),
					passed: false,
					message: "".into(),
				},
				AssertionReport {
					name: "test_short_assertion".into(),
					passed: false,
					message: "assertion failed for some reasons".into(),
				},
				AssertionReport {
					name: "test_multi_line_assertion".into(),
					passed: false,
					message: indoc::indoc!(
						"
						multi line assertion failed for some reasons
						second line
						  third line

						fourth line
					"
					)
					.into(),
				},
			],
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
