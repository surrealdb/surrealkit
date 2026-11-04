declare const q: ReturnType<typeof db.query<"SELECT * FROM skipped_declaration">>;
db.query("SELECT * FROM skipped_declaration");
