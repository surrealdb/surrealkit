DEFINE TABLE OVERWRITE person SCHEMAFULL
	PERMISSIONS FOR select, create, update, delete WHERE $access = 'account';
DEFINE FIELD OVERWRITE name ON person TYPE string;
DEFINE FIELD OVERWRITE team ON person TYPE string;
DEFINE FIELD OVERWRITE joined ON person TYPE datetime DEFAULT time::now();
