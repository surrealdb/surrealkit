DEFINE FUNCTION OVERWRITE fn::organization::permissible($account: option<record<user>>, $organization: option<record<organization>>, $permission: string) {
	IF $account == NONE OR $organization == NONE { RETURN false };

	LET $roles = (SELECT VALUE role FROM employee_of
		WHERE out = $organization AND in = $account AND status = "active");
	FOR $role IN $roles {
		IF fn::organization::role::contains($role, "organization/manage") { RETURN true };
		IF fn::organization::role::contains($role, $permission) { RETURN true };
	};

	LET $parents = (SELECT VALUE in FROM has_subsidiary WHERE out = $organization AND status = "active");
	FOR $parent IN $parents {
		LET $teams = fn::account::organization::teams($account, $parent);
		LET $unit = fn::account::organization::unit($account, $parent);
		LET $grants = SELECT role FROM organization_delegation_grant
			WHERE in = $parent AND out = $organization
				AND (subject = $account OR subject IN $teams
					OR ($unit != NONE AND fn::organization::unit::descendant($unit, subject)));
		FOR $g IN $grants {
			IF fn::organization::role::contains($g.role, "organization/manage")
				OR fn::organization::role::contains($g.role, $permission) { RETURN true };
		};
	};

	RETURN false;
};
