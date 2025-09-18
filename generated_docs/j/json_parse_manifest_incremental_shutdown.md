# json_parse_manifest_incremental_shutdown

## Location
src/common/parse_manifest.c: 169 - 184

## Overview
Properly cleans up and frees all memory associated with an incremental JSON manifest parser state.

## Definition


## Detailed Description
This function performs cleanup operations for an incremental manifest parser that was previously initialized with json_parse_manifest_incremental_init. It systematically frees all allocated memory including the semantic state, JSON lexical context, and the incremental state structure itself. The manifest hash context is expected to be already freed by the caller before this function is invoked.

This is the complementary cleanup function to json_parse_manifest_incremental_init and should always be called when incremental parsing is complete to prevent memory leaks.

## Parameters / Member Variables
- : Pointer to JsonManifestParseIncrementalState to be freed and cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
  - freeJsonLexContext
- Called from (representative examples):
  - [FinalizeIncrementalManifest](../F/FinalizeIncrementalManifest.md) (src/backend/backup/basebackup_incremental.c:245)
  - [load_backup_manifest](../l/load_backup_manifest.md) (src/bin/pg_combinebackup/load_manifest.c:211)
  - [parse_manifest_file](../p/parse_manifest_file.md) (src/bin/pg_verifybackup/pg_verifybackup.c:489)

## Notes and Other Information
- The manifest_ctx (cryptographic hash context) should be freed by the caller before calling this function
- This function does not return any value (void return type)
- Failure to call this function will result in memory leaks
- Should be called in error paths as well as successful completion paths
- Part of PostgreSQL's backup manifest processing cleanup infrastructure