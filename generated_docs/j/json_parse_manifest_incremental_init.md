# json_parse_manifest_incremental_init

## Location
[src/common/parse_manifest.c:129-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L129-L168)

## Overview
Sets up and initializes an incremental JSON manifest parser state for processing PostgreSQL backup manifests in chunks.

## Definition

```c
JsonManifestParseIncrementalState *
json_parse_manifest_incremental_init(JsonManifestParseContext *context)
```
## Detailed Description
This function creates and initializes a new incremental parsing state for JSON manifest files. It sets up all the necessary components for streaming JSON parsing including the lexical context, semantic handlers, and cryptographic hash computation for manifest verification. The incremental parser allows processing large manifest files in chunks rather than loading the entire file into memory at once.

The function configures semantic handlers for various JSON elements (objects, arrays, fields, scalars) and initializes a SHA256 hash context for computing the manifest checksum during parsing.

## Parameters / Member Variables
- `*context`: Pointer to JsonManifestParseContext containing error callback and other parsing context information
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - makeJsonLexContextIncremental
  - [json_manifest_object_start](json_manifest_object_start.md)
  - [json_manifest_object_end](json_manifest_object_end.md)
  - [json_manifest_array_start](json_manifest_array_start.md)
  - [json_manifest_array_end](json_manifest_array_end.md)
  - [json_manifest_object_field_start](json_manifest_object_field_start.md)
  - [json_manifest_scalar](json_manifest_scalar.md)
  - [pg_cryptohash_create](../p/pg_cryptohash_create.md)
  - [pg_cryptohash_init](../p/pg_cryptohash_init.md)
  - PG_SHA256
  - PG_UTF8
  - JM_EXPECT_TOPLEVEL_START
- Called from (representative examples):
  - [load_backup_manifest](../l/load_backup_manifest.md) (src/bin/pg_combinebackup/load_manifest.c:178)
  - [parse_manifest_file](../p/parse_manifest_file.md) (src/bin/pg_verifybackup/pg_verifybackup.c:455)

## Notes and Other Information
- Returns a fully initialized JsonManifestParseIncrementalState pointer
- Memory allocation failures or hash initialization errors trigger the error callback
- The parser state tracks version field presence and maintains current parsing state
- Designed for UTF-8 encoded JSON input with strict parsing enabled
- Part of PostgreSQL's backup manifest processing infrastructure

## Simplified Source

```c
JsonManifestParseIncrementalState *
json_parse_manifest_incremental_init(JsonManifestParseContext *context)
{
    JsonManifestParseIncrementalState *incstate;
    JsonManifestParseState *parse;
    pg_cryptohash_ctx *manifest_ctx;

    // Allocate incremental state and parse state structures
    incstate = palloc(sizeof(JsonManifestParseIncrementalState));
    parse = palloc(sizeof(JsonManifestParseState));

    // Initialize parse state
    parse->context = context;
    parse->state = JM_EXPECT_TOPLEVEL_START;
    parse->saw_version_field = false;

    // Set up incremental JSON lexer for UTF-8 with strict parsing
    makeJsonLexContextIncremental(&(incstate->lex), PG_UTF8, true);

    // Configure semantic action handlers
    incstate->sem.semstate = parse;
    incstate->sem.object_start = json_manifest_object_start;
    incstate->sem.object_end = json_manifest_object_end;
    incstate->sem.array_start = json_manifest_array_start;
    incstate->sem.array_end = json_manifest_array_end;
    incstate->sem.object_field_start = json_manifest_object_field_start;
    incstate->sem.object_field_end = NULL;
    incstate->sem.array_element_start = NULL;
    incstate->sem.array_element_end = NULL;
    incstate->sem.scalar = json_manifest_scalar;

    // Initialize SHA256 hash context for manifest verification
    manifest_ctx = pg_cryptohash_create(PG_SHA256);
    if (manifest_ctx == NULL)
        context->error_cb(context, "out of memory");
    if (pg_cryptohash_init(manifest_ctx) < 0)
        context->error_cb(context, "could not initialize checksum of manifest");
    incstate->manifest_ctx = manifest_ctx;

    return incstate;
}
```