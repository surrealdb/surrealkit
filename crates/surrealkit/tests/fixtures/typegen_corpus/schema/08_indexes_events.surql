-- Typegen golden corpus, module 08: analyzers, every INDEX kind, and EVENTs on `doc`.

DEFINE TABLE doc SCHEMAFULL;
DEFINE FIELD title ON doc TYPE string;
DEFINE FIELD body ON doc TYPE string;
DEFINE FIELD code ON doc TYPE string;
DEFINE FIELD owner ON doc TYPE record<person>;
DEFINE FIELD status ON doc TYPE 'open' | 'closed' DEFAULT 'open';
DEFINE FIELD embedding ON doc TYPE array<float, 4>;
DEFINE FIELD touched ON doc TYPE bool DEFAULT false;

DEFINE ANALYZER simple TOKENIZERS blank, class FILTERS lowercase, ascii, snowball(english) COMMENT 'plain text';
DEFINE ANALYZER prepped FUNCTION fn::prep TOKENIZERS blank FILTERS lowercase, edgengram(2, 10);

DEFINE INDEX idx_title ON doc FIELDS title;
DEFINE INDEX uniq_code ON doc FIELDS code UNIQUE COMMENT 'one doc per code';
DEFINE INDEX idx_owner_status ON doc FIELDS owner, status;
DEFINE INDEX ft_body ON doc FIELDS body FULLTEXT ANALYZER simple BM25(1.2, 0.75) HIGHLIGHTS;
DEFINE INDEX hnsw_embedding ON doc FIELDS embedding HNSW DIMENSION 4 DIST COSINE TYPE F32 EFC 150 M 12;
DEFINE INDEX diskann_embedding ON doc FIELDS embedding DISKANN DIMENSION 4 DIST EUCLIDEAN;
DEFINE INDEX cnt_all ON doc COUNT;
DEFINE INDEX cnt_open ON doc COUNT WHERE status = 'open';
DEFINE INDEX idx_title_concurrent ON doc FIELDS title CONCURRENTLY;
DEFINE INDEX uniq_email ON person FIELDS email UNIQUE;

-- Synchronous event with a WHEN guard, async event with retry/maxdepth and a block
-- body, and an event with several THEN expressions and no guard.
DEFINE EVENT on_create ON doc WHEN $event = 'CREATE' THEN (CREATE audit_log SET doc = $after.id);
DEFINE EVENT on_status ON doc ASYNC RETRY 3 MAXDEPTH 2
	WHEN $before.status != $after.status
	THEN {
		CREATE audit_log SET doc = $after.id, note = string::concat($before.status, ' -> ', $after.status);
	}
	COMMENT 'status transitions';
DEFINE EVENT on_any ON doc THEN (UPDATE $after.id SET touched = true), (CREATE audit_log SET note = 'any');
