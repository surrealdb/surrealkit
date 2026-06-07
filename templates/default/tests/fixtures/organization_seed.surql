CREATE user:alice SET email = "alice@example.com", name = "Alice";
CREATE user:bob   SET email = "bob@example.com",   name = "Bob";
CREATE user:carol SET email = "carol@example.com", name = "Carol";

CREATE organization:acme SET name = "Acme", slug = "acme", owner = user:alice;

LET $member_role = CREATE ONLY organization_role SET
	name = "Member",
	organization = organization:acme,
	description = "Standard member",
	permissions = [organization_permission:employees_view];
RELATE user:bob->employee_of->organization:acme CONTENT {
	role: $member_role.id,
	title: "Member",
	status: "active"
};
