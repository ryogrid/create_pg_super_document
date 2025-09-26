# JsonManifestParseIncrementalState

## Location
[src/common/parse_manifest.c:95-128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L95-L128)

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
  - [JsonLexContext](JsonLexContext.md)
  - [JsonSemAction](JsonSemAction.md)
  - [pg_cryptohash_ctx](../p/pg_cryptohash_ctx.md)
  - [json_manifest_object_start](../j/json_manifest_object_start.md)
  - [json_manifest_object_end](../j/json_manifest_object_end.md)
  - [json_manifest_array_start](../j/json_manifest_array_start.md)
  - [json_manifest_array_end](../j/json_manifest_array_end.md)
  - [json_manifest_object_field_start](../j/json_manifest_object_field_start.md)
  - [json_manifest_scalar](../j/json_manifest_scalar.md)
  - JsonParseErrorType
  - [JsonTokenType](JsonTokenType.md)
  - [verify_manifest_checksum](../v/verify_manifest_checksum.md)
  - [json_manifest_parse_failure](../j/json_manifest_parse_failure.md)

- Called from (representative examples):
  - [json_parse_manifest_incremental_init](../j/json_parse_manifest_incremental_init.md) (src/common/parse_manifest.c:131)
  - [json_parse_manifest_incremental_shutdown](../j/json_parse_manifest_incremental_shutdown.md) (src/common/parse_manifest.c:169)
  - [json_parse_manifest_incremental_chunk](../j/json_parse_manifest_incremental_chunk.md) (src/common/parse_manifest.c:185)
  - [IncrementalBackupInfo](../I/IncrementalBackupInfo.md) (src/backend/backup/basebackup_incremental.c:127)
  - [load_backup_manifest](../l/load_backup_manifest.md) (src/bin/pg_combinebackup/load_manifest.c:176)
  - [parse_manifest_file](../p/parse_manifest_file.md) (src/bin/pg_verifybackup/pg_verifybackup.c:453)

## Notes and Other Information
- The typedef declaration appears in parse_manifest.h (line 23) while the actual struct definition is in parse_manifest.c (lines 95-100)
- This structure is specifically designed for incremental parsing to handle memory constraints when processing large backup manifest files
- The manifest_ctx member is freed externally before the structure itself is deallocated, as noted in the shutdown function
- The structure is used in conjunction with JsonManifestParseContext which provides callback functions and error handling for the parsing process
- Key usage locations include backup verification tools, backup combination utilities, and incremental backup processing systems