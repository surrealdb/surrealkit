UPSERT organization_permission:organization_manage       SET name = "organization/manage",       description = "Full control of the organization";
UPSERT organization_permission:organization_subsidiaries SET name = "organization/subsidiaries", description = "Manage subsidiaries and delegations";
UPSERT organization_permission:employees_view            SET name = "employees/view",            description = "View employees";
UPSERT organization_permission:employees_manage          SET name = "employees/manage",          description = "Invite, update, and remove employees";
UPSERT organization_permission:roles_read                SET name = "roles/read",                description = "View roles";
UPSERT organization_permission:roles_manage              SET name = "roles/manage",              description = "Create, update, and delete roles";
UPSERT organization_permission:roles_assign              SET name = "roles/assign",              description = "Assign roles to employees";
