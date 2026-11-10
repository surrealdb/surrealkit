-- One task blocks another.
DEFINE TABLE blocks TYPE RELATION IN task OUT task SCHEMAFULL PERMISSIONS FOR select FULL;
