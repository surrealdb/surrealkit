DEFINE FUNCTION OVERWRITE fn::account::organization::teams($account: record<user>, $organization: record<organization>) {
	LET $employees = (SELECT VALUE id FROM employee_of WHERE in = $account AND out = $organization);
	RETURN (SELECT VALUE out FROM member_of WHERE in IN $employees AND out.organization = $organization);
};

DEFINE FUNCTION OVERWRITE fn::team::get_role($account: record<user>, $team: record<team>) {
	LET $employees = (SELECT VALUE id FROM employee_of WHERE in = $account AND status = "active");
	RETURN (SELECT VALUE role FROM member_of WHERE in IN $employees AND out = $team LIMIT 1)[0];
};

DEFINE FUNCTION OVERWRITE fn::team::member_of($account: option<record<user>>, $team: option<record<team>>, $role: option<string>) {
	IF $account == NONE OR $team == NONE { RETURN false };
	LET $current = fn::team::get_role($account, $team);
	IF $current == NONE { RETURN false };
	LET $required = IF $role == "maintainer" THEN ["maintainer"] ELSE ["maintainer", "member"] END;
	RETURN $current IN $required;
};

DEFINE FUNCTION OVERWRITE fn::team::members($team: record<team>) {
	RETURN SELECT in AS employee, in.in AS account, role, joined_at FROM member_of WHERE out = $team;
};
