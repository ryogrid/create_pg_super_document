# json_parse_manifest_incremental_chunk

## Location
[src/common/parse_manifest.c:185-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L185-L226)

## Overview
Processes a chunk of JSON manifest data incrementally, updating the hash and parsing state for streaming manifest processing.

## Definition

```c
void
json_parse_manifest_incremental_chunk(JsonManifestParseIncrementalState *incstate,
									  const char *chunk, size_t size, bool is_last)
```
## Detailed Description
This function is the core of incremental JSON manifest parsing, processing individual chunks of manifest data as they become available. It performs incremental JSON parsing using the PostgreSQL JSON parser, maintains a running cryptographic hash of the manifest content (except for the final chunk which contains the checksum), and validates parsing state transitions.

The function handles both intermediate chunks (where JSON parsing may be incomplete) and the final chunk (which must result in complete JSON parsing and EOF state). For non-final chunks, it updates the manifest checksum hash. For the final chunk, it verifies the manifest checksum against the computed hash.

## Parameters / Member Variables
- `*incstate`: Pointer to the incremental parser state containing lexical context, semantic handlers, and hash context
- `*chunk`: Pointer to the data chunk to be processed
- `size`: Size in bytes of the chunk to process
- `is_last`: Boolean flag indicating whether this is the final chunk of the manifest
## Dependencies
- Functions called/Symbols referenced:
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md)
  - [json_manifest_parse_failure](json_manifest_parse_failure.md)
  - [json_errdetail](json_errdetail.md)
  - [pg_cryptohash_update](../p/pg_cryptohash_update.md)
  - [verify_manifest_checksum](../v/verify_manifest_checksum.md)
  - JSON_SUCCESS
  - JSON_INCOMPLETE
  - JM_EXPECT_EOF
- Called from (representative examples):
  - [AppendIncrementalManifestData](../A/AppendIncrementalManifestData.md) (src/backend/backup/basebackup_incremental.c:210)
  - [FinalizeIncrementalManifest](../F/FinalizeIncrementalManifest.md) (src/backend/backup/basebackup_incremental.c:237)
  - [load_backup_manifest](../l/load_backup_manifest.md) (src/bin/pg_combinebackup/load_manifest.c:207)
  - [parse_manifest_file](../p/parse_manifest_file.md) (src/bin/pg_verifybackup/pg_verifybackup.c:484)

## Notes and Other Information
- Expects JSON_INCOMPLETE for non-final chunks and JSON_SUCCESS for the final chunk
- The final chunk must result in JM_EXPECT_EOF parser state
- [Hash](../H/Hash.md) computation excludes the final chunk since it contains the checksum itself
- Parse errors trigger the context error callback with detailed error information
- Checksum verification is only performed on the final chunk
- Critical component of PostgreSQL's streaming backup manifest processing

## Simplified Source

```c
// Simplified version of json_parse_manifest_incremental_chunk
void json_parse_manifest_incremental_chunk(JsonManifestParseIncrementalState *incstate,
                                          const char *chunk, size_t size, bool is_last)
{
    JsonManifestParseState *parse = incstate->sem.semstate;
    JsonManifestParseContext *context = parse->context;

    // Parse the JSON chunk incrementally
    JsonParseErrorType result = pg_parse_json_incremental(&(incstate->lex), &(incstate->sem),
                                                         chunk, size, is_last);

    // Check if parsing result matches expectation
    JsonParseErrorType expected = is_last ? JSON_SUCCESS : JSON_INCOMPLETE;
    if (result != expected) {
        json_manifest_parse_failure(context, json_errdetail(result, &(incstate->lex)));
    }

    // Verify final state for last chunk
    if (is_last && parse->state != JM_EXPECT_EOF) {
        json_manifest_parse_failure(context, "manifest ended unexpectedly");
    }

    // Update checksum for non-final chunks, verify for final chunk
    if (!is_last) {
        // Update running hash with chunk data
        if (pg_cryptohash_update(incstate->manifest_ctx, (const uint8 *) chunk, size) < 0) {
            context->error_cb(context, "could not update checksum of manifest");
        }
    } else {
        // Verify the complete manifest checksum
        verify_manifest_checksum(parse, chunk, size, incstate->manifest_ctx);
    }
}
```

Key simplifications made:
- Consolidated variable declarations for better readability
- Added descriptive comments explaining each major step
- Clarified the conditional logic flow between intermediate and final chunks
- Simplified error handling flow while preserving essential checks
- Made the checksum update vs verification logic more explicit