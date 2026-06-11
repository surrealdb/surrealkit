DEFINE FUNCTION OVERWRITE fn::organization::unit::path($unit: record<organization_unit>) {
	LET $x = (SELECT organization, parent FROM ONLY $unit);
	IF $x == NONE { RETURN "" };
	IF $x.parent == NONE { RETURN "/" + record::id($unit) + "/" };
	LET $parent_path = (SELECT VALUE path FROM ONLY $x.parent);
	IF $parent_path == NONE OR $parent_path == "" { RETURN "" };
	RETURN $parent_path + record::id($unit) + "/";
};

DEFINE FUNCTION OVERWRITE fn::organization::unit::descendant($child: record<organization_unit>, $parent: record<organization_unit>) {
	LET $c = (SELECT VALUE path FROM ONLY $child);
	LET $a = (SELECT VALUE path FROM ONLY $parent);
	IF $c == NONE OR $a == NONE { RETURN false };
	RETURN string::starts_with($c, $a);
};

DEFINE FUNCTION OVERWRITE fn::account::organization::unit($account: record<user>, $organization: record<organization>) {
	RETURN (SELECT VALUE unit FROM ONLY employee_of
		WHERE in = $account AND out = $organization AND status = "active");
};
