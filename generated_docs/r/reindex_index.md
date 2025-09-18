# reindex_index

## Location
src/backend/catalog/index.c: 3547 - 3886

## Overview
The  function recreates a single index from scratch, handling all aspects of index reconstruction including validation, locking, storage management, and constraint checking.

## Definition


## Detailed Description
This function performs a complete reconstruction of a single index specified by its OID. It handles the complex process of safely rebuilding an index while maintaining database consistency and transaction safety. The function manages security contexts, progress reporting, tablespace changes, and constraint validation. It supports both regular and "missing ok" modes for handling potentially non-existent relations.

The reindexing process involves:
1. Opening and locking the parent heap relation
2. Setting up proper security context and GUC settings
3. Opening and exclusively locking the target index
4. Validating that the index can be reindexed (checking for partitioned indexes, temp tables, etc.)
5. Optionally moving the index to a new tablespace
6. Suppressing index usage during reconstruction
7. Creating new physical storage and rebuilding the index
8. Updating index validity flags in the system catalog
9. Cleaning up locks and security context

## Parameters / Member Variables
- : Optional REINDEX statement for event trigger collection; can be NULL
- : Object identifier of the index to be reindexed
- : If true, skips uniqueness and exclusion constraint validation during rebuild
- : Storage persistence type for the new index files
- : Reindex parameters including options for progress reporting, verbosity, missing ok behavior, and target tablespace

## Dependencies
- Functions called/Symbols referenced:
  - IndexGetRelation: Gets heap relation OID from index OID
  - table_open/try_table_open: Opens the parent heap relation
  - index_open/try_index_open: Opens the target index relation
  - SetUserIdAndSecContext: Manages security context for index functions
  - CheckRelationTableSpaceMove: Validates tablespace move operations
  - CheckTableNotInUse: Ensures no active usage of the index
  - SetRelationTableSpace: Updates tablespace information
  - RelationDropStorage: Schedules old storage file deletion
  - SetReindexProcessing/ResetReindexProcessing: Controls index usage suppression
  - index_build: Performs the actual index reconstruction
  - BuildIndexInfo: Creates index metadata structure
- Called from (representative examples):
  - reindex_relation: For reindexing all indexes of a relation
  - ReindexIndex: Direct command interface for REINDEX INDEX
  - ReindexMultipleInternal: Batch reindexing operations

## Notes and Other Information
- Requires AccessExclusiveLock on the target index to prevent concurrent access
- Automatically promotes predicate locks to heap relation level during reconstruction
- Supports progress reporting through the PostgreSQL statistics collector
- Can handle invalid indexes from failed CREATE INDEX CONCURRENTLY operations
- Prevents reindexing of partitioned indexes (which have no physical storage)
- Includes comprehensive error checking for temporary tables and TOAST indexes
- Manages transaction-level GUC changes and security context properly
- Updates system catalogs to mark rebuilt indexes as valid and ready