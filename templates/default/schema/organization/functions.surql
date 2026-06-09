DEFINE FUNCTION OVERWRITE fn::organization::role::contains($role: option<record<organization_role>>, $permission: string) {
	IF $role == NONE { RETURN false };
	LET $names = (SELECT VALUE permissions.name FROM ONLY $role);
	IF $names == NONE { RETURN false };
	RETURN $permission IN $names;
};

DEFINE FUNCTION OVERWRITE fn::organization::employee($account: option<record>, $organization: record<organization>) {
	IF $account == NONE { RETURN false };
	RETURN (SELECT VALUE id FROM employee_of
		WHERE in = $account AND out = $organization AND status = "active" LIMIT 1) != [];
};

DEFINE FUNCTION OVERWRITE fn::account::organization::unit($account: record<user>, $organization: record<organization>) {
	RETURN NONE;
};

DEFINE FUNCTION OVERWRITE fn::account::organization::teams($account: record<user>, $organization: record<organization>) {
	RETURN [];
};

DEFINE FUNCTION OVERWRITE fn::organization::permissible($account: option<record<user>>, $organization: option<record<organization>>, $permission: string) {
	IF $account == NONE OR $organization == NONE { RETURN false };

	LET $roles = (SELECT VALUE role FROM employee_of
		WHERE out = $organization AND in = $account AND status = "active");
	FOR $role IN $roles {
		IF fn::organization::role::contains($role, "organization/manage") { RETURN true };
		IF fn::organization::role::contains($role, $permission) { RETURN true };
	};

	RETURN false;
};
