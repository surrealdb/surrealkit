-- Typegen golden corpus, module 31 (4.0.x spelling): a Surrealism module.
-- Requires `--allow-experimental=surrealism` and the `avatars` bucket (module 20).
-- 4.0 calls modules extensions and, when the embedded engine cannot read the archive to
-- compute it, requires `DIGEST`.

DEFINE MODULE mod::demo FROM f"avatars:/demo.wasm" UNSIGNED DIGEST 'sha256:0000000000000000000000000000000000000000000000000000000000000000';
