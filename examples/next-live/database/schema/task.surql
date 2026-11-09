DEFINE TABLE task SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD title ON task TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD status ON task TYPE "todo" | "doing" | "done" DEFAULT "todo";
DEFINE FIELD owner ON task TYPE option<record<person>>;
DEFINE FIELD created ON task TYPE datetime DEFAULT time::now() READONLY;
