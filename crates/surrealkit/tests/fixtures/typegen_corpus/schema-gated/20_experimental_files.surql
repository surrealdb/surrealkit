-- Typegen golden corpus, module 20: file buckets.
-- Requires the experimental `files` capability (`--allow-experimental=files`).

DEFINE BUCKET avatars BACKEND "memory" PERMISSIONS FULL COMMENT 'profile images';
DEFINE BUCKET docs READONLY BACKEND "memory";

DEFINE TABLE attachment SCHEMAFULL;
DEFINE FIELD avatar ON attachment TYPE option<file<avatars>>;
DEFINE FIELD files ON attachment TYPE array<file<avatars | docs>> DEFAULT [];
DEFINE FIELD any_file ON attachment TYPE option<file>;
