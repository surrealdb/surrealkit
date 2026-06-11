UPSERT organization_permission:team_view   SET name = "team/view",   description = "View teams";
UPSERT organization_permission:team_create SET name = "team/create", description = "Create teams";
UPSERT organization_permission:team_update SET name = "team/update", description = "Update teams";
UPSERT organization_permission:team_delete SET name = "team/delete", description = "Delete teams";
UPSERT organization_permission:team_manage SET name = "team/manage", description = "Manage all teams and their members";
