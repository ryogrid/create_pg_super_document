# json_parse_manifest

## Location
[src/common/parse_manifest.c:227-275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L227-L275)

## Overview
Main entry point for parsing a complete JSON-format backup manifest from a memory buffer in a single operation.

## Definition

```c
void
json_parse_manifest(JsonManifestParseContext *context, const char *buffer,
					size_t size)
```
## Detailed Description
This function provides a complete, non-incremental JSON manifest parsing solution for cases where the entire manifest is available in memory. It sets up a JSON lexical context from the provided buffer, configures semantic action handlers for various JSON elements, and performs complete parsing in one pass.

Unlike the incremental parsing functions, this operates on the entire manifest at once and handles checksum verification after parsing is complete. It's suitable for smaller manifests or when the entire manifest content is readily available in memory.

The function automatically invokes the per-file callback for each file entry found in the manifest and the error callback if any parsing errors occur.

## Parameters / Member Variables
- `*context`: Pointer to JsonManifestParseContext containing callbacks and parsing configuration
- `*buffer`: Pointer to memory buffer containing the complete JSON manifest data
- `size`: Size in bytes of the manifest data in the buffer
## Dependencies
- Functions called/Symbols referenced:
  - [makeJsonLexContextCstringLen](../m/makeJsonLexContextCstringLen.md)
  - [json_manifest_object_start](json_manifest_object_start.md)
  - [json_manifest_object_end](json_manifest_object_end.md)
  - [json_manifest_array_start](json_manifest_array_start.md)
  - [json_manifest_array_end](json_manifest_array_end.md)
  - [json_manifest_object_field_start](json_manifest_object_field_start.md)
  - [json_manifest_scalar](json_manifest_scalar.md)
  - [pg_parse_json](../p/pg_parse_json.md)
  - [json_manifest_parse_failure](json_manifest_parse_failure.md)
  - [json_errdetail](json_errdetail.md)
  - [verify_manifest_checksum](../v/verify_manifest_checksum.md)
  - [freeJsonLexContext](../f/freeJsonLexContext.md)
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

## Simplified Source

```c
void json_parse_manifest(JsonManifestParseContext *context, const char *buffer, size_t size) {
    JsonLexContext *lex;
    JsonParseErrorType json_error;
    JsonSemAction sem;
    JsonManifestParseState parse;

    // Initialize parse state
    parse.context = context;
    parse.state = JM_EXPECT_TOPLEVEL_START;
    parse.saw_version_field = false;

    // Create JSON lexer for the buffer
    lex = makeJsonLexContextCstringLen(NULL, buffer, size, PG_UTF8, true);

    // Set up semantic action callbacks for JSON elements
    sem.semstate = &parse;
    sem.object_start = json_manifest_object_start;
    sem.object_end = json_manifest_object_end;
    sem.array_start = json_manifest_array_start;
    sem.array_end = json_manifest_array_end;
    sem.object_field_start = json_manifest_object_field_start;
    sem.scalar = json_manifest_scalar;
    // Set unused callbacks to NULL
    sem.object_field_end = NULL;
    sem.array_element_start = NULL;
    sem.array_element_end = NULL;

    // Parse the JSON manifest
    json_error = pg_parse_json(lex, &sem);
    if (json_error != JSON_SUCCESS)
        json_manifest_parse_failure(context, json_errdetail(json_error, lex));

    // Verify parsing completed correctly
    if (parse.state != JM_EXPECT_EOF)
        json_manifest_parse_failure(context, "manifest ended unexpectedly");

    // Verify manifest checksum
    verify_manifest_checksum(&parse, buffer, size, NULL);

    // Clean up
    freeJsonLexContext(lex);
}
```