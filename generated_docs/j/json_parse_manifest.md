# json_parse_manifest

## Location
[src/common/parse_manifest.c:227-275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L227-L275)

## Overview
Main entry point for parsing a complete JSON-format backup manifest from a memory buffer in a single operation.

## Definition


## Detailed Description
This function provides a complete, non-incremental JSON manifest parsing solution for cases where the entire manifest is available in memory. It sets up a JSON lexical context from the provided buffer, configures semantic action handlers for various JSON elements, and performs complete parsing in one pass.

Unlike the incremental parsing functions, this operates on the entire manifest at once and handles checksum verification after parsing is complete. It's suitable for smaller manifests or when the entire manifest content is readily available in memory.

The function automatically invokes the per-file callback for each file entry found in the manifest and the error callback if any parsing errors occur.

## Parameters / Member Variables
- : Pointer to JsonManifestParseContext containing callbacks and parsing configuration
- : Pointer to memory buffer containing the complete JSON manifest data
- : Size in bytes of the manifest data in the buffer

## Dependencies
- Functions called/Symbols referenced:
  - [makeJsonLexContextCstringLen](../m/makeJsonLexContextCstringLen.md)
  - [json_manifest_object_start](json_manifest_object_start.md)
  - json_manifest_object_end
  - json_manifest_array_start
  - json_manifest_array_end
  - json_manifest_object_field_start
  - json_manifest_scalar
  - [pg_parse_json](../p/pg_parse_json.md)
  - json_manifest_parse_failure
  - json_errdetail
  - verify_manifest_checksum
  - freeJsonLexContext
  - JSON_SUCCESS
  - JM_EXPECT_TOPLEVEL_START
  - JM_EXPECT_EOF
  - PG_UTF8
- Called from (representative examples):
  - [load_backup_manifest](../l/load_backup_manifest.md) (src/bin/pg_combinebackup/load_manifest.c:171)
  - [parse_manifest_file](../p/parse_manifest_file.md) (src/bin/pg_verifybackup/pg_verifybackup.c:448)

## Notes and Other Information
- Requires the entire manifest to be loaded into memory before parsing
- Performs complete checksum verification after parsing completion
- Expects the parser to end in JM_EXPECT_EOF state for successful completion
- Uses strict UTF-8 JSON parsing with full semantic validation
- Automatically cleans up the JSON lexical context before returning
- Alternative to incremental parsing for simpler use cases or smaller manifests
- Part of PostgreSQL's backup manifest processing infrastructure