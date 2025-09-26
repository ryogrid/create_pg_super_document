# JsonManifestParseIncrementalState

## Location
src/common/parse_manifest.c: 95 - 128

## Overview
A structure that maintains state for incremental parsing of PostgreSQL backup manifest files in JSON format, enabling the processing of large manifest files in chunks rather than loading them entirely into memory.

## Definition
```c
struct JsonManifestParseIncrementalState
{
    JsonLexContext lex;
    JsonSemAction sem;
    pg_cryptohash_ctx *manifest_ctx;
};
```

## Detailed Description
JsonManifestParseIncrementalState is a state container used for parsing JSON backup manifest files incrementally. It is designed to handle large manifest files by processing them in chunks, which is essential for memory efficiency when dealing with backup manifests that can be very large. The structure combines JSON lexical analysis, semantic actions, and cryptographic hash computation to provide a complete incremental parsing solution.

The structure is created by `json_parse_manifest_incremental_init()`, used with `json_parse_manifest_incremental_chunk()` for processing data chunks, and cleaned up with `json_parse_manifest_incremental_shutdown()`. During initialization, it sets up the JSON lexer for incremental processing, configures semantic action callbacks for handling different JSON elements (objects, arrays, scalars), and initializes a SHA-256 cryptographic context for manifest checksum verification.

## Parameters / Member Variables
- `lex`: JsonLexContext structure that maintains the state of JSON lexical analysis during incremental parsing
- `sem`: JsonSemAction structure containing callback functions for handling different JSON semantic elements (object start/end, array start/end, field start, scalars)
- `manifest_ctx`: Pointer to a cryptographic hash context (SHA-256) used for computing and verifying the manifest checksum during parsing

## Dependencies
- Functions called/Symbols referenced:
  - JsonLexContext
  - JsonSemAction
  - pg_cryptohash_ctx
  - json_manifest_object_start
  - json_manifest_object_end
  - json_manifest_array_start
  - json_manifest_array_end
  - json_manifest_object_field_start
  - json_manifest_scalar
  - JsonParseErrorType
  - JsonTokenType
  - verify_manifest_checksum
  - json_manifest_parse_failure

- Called from (representative examples):
  - json_parse_manifest_incremental_init (src/common/parse_manifest.c:131)
  - json_parse_manifest_incremental_shutdown (src/common/parse_manifest.c:169)
  - json_parse_manifest_incremental_chunk (src/common/parse_manifest.c:185)
  - IncrementalBackupInfo (src/backend/backup/basebackup_incremental.c:127)
  - load_backup_manifest (src/bin/pg_combinebackup/load_manifest.c:176)
  - parse_manifest_file (src/bin/pg_verifybackup/pg_verifybackup.c:453)

## Notes and Other Information
- The typedef declaration appears in parse_manifest.h (line 23) while the actual struct definition is in parse_manifest.c (lines 95-100)
- This structure is specifically designed for incremental parsing to handle memory constraints when processing large backup manifest files
- The manifest_ctx member is freed externally before the structure itself is deallocated, as noted in the shutdown function
- The structure is used in conjunction with JsonManifestParseContext which provides callback functions and error handling for the parsing process
- Key usage locations include backup verification tools, backup combination utilities, and incremental backup processing systems