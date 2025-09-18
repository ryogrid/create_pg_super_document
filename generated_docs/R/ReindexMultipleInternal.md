# ReindexMultipleInternal

## Location
src/backend/commands/indexcmds.c: 3311 - 3436

## Overview
ReindexMultipleInternal reindexes a list of relations, with each relation being processed in its own separate transaction to ensure proper isolation and error recovery.

## Definition


## Detailed Description
This internal function handles the bulk reindexing of multiple database relations by processing each relation in its own transaction context. The function commits the current transaction at the beginning and starts fresh transactions for each relation to be reindexed. This approach ensures that if one relation fails to reindex, it doesn't affect the processing of other relations.

The function performs several key operations for each relation:
- Validates that the relation still exists before attempting to reindex
- Checks permissions for tablespace operations when specified
- Handles different reindexing strategies based on relation type and options (concurrent vs. standard reindexing)
- Provides appropriate verbose output when requested

The function supports both concurrent and standard reindexing modes, automatically choosing the appropriate reindex function based on relation type (index vs. table) and reindexing options.

## Parameters / Member Variables
- : ReindexStmt structure containing the original REINDEX statement information
- : List of relation OIDs to be reindexed
- : ReindexParams structure containing reindexing options and parameters

## Dependencies
- Functions called/Symbols referenced:
  - PopActiveSnapshot
  - CommitTransactionCommand
  - StartTransactionCommand
  - PushActiveSnapshot
  - GetTransactionSnapshot
  - SearchSysCacheExists1
  - object_aclcheck
  - get_rel_relkind
  - get_rel_persistence
  - ReindexRelationConcurrently
  - reindex_index
  - reindex_relation
  - get_namespace_name
  - get_rel_name
- Called from (representative examples):
  - ReindexMultipleTables
  - ReindexPartitions

## Notes and Other Information
- The function is static (internal to indexcmds.c) and designed for internal use within the reindexing subsystem
- Each relation is processed in its own transaction to provide isolation and error recovery
- The function handles partitioned relations by asserting they should never be processed directly (their leaves should be built first)
- Supports both concurrent and standard reindexing modes with appropriate option handling
- Includes proper permission checking for tablespace operations
- Provides verbose output capability for monitoring reindex operations
- Uses REINDEXOPT_MISSING_OK flag to handle relations that may have been dropped during processing