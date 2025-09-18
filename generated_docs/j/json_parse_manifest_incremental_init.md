# json_parse_manifest_incremental_init

## Location
src/common/parse_manifest.c: 129 - 168

## Overview
Sets up and initializes an incremental JSON manifest parser state for processing PostgreSQL backup manifests in chunks.

## Definition


## Detailed Description
This function creates and initializes a new incremental parsing state for JSON manifest files. It sets up all the necessary components for streaming JSON parsing including the lexical context, semantic handlers, and cryptographic hash computation for manifest verification. The incremental parser allows processing large manifest files in chunks rather than loading the entire file into memory at once.

The function configures semantic handlers for various JSON elements (objects, arrays, fields, scalars) and initializes a SHA256 hash context for computing the manifest checksum during parsing.

## Parameters / Member Variables
- : Pointer to JsonManifestParseContext containing error callback and other parsing context information

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - makeJsonLexContextIncremental
  - [json_manifest_object_start](json_manifest_object_start.md)
  - json_manifest_object_end
  - json_manifest_array_start
  - json_manifest_array_end
  - json_manifest_object_field_start
  - json_manifest_scalar
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