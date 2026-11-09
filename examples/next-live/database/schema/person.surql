-- Readable by anyone (the browser connects as a guest); written only by the server.
DEFINE TABLE person SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD name ON person TYPE string;
