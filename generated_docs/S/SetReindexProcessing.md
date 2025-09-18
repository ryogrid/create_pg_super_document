# SetReindexProcessing

## Location
src/backend/catalog/index.c: 4090 - 4108

## Overview
Sets the global state flags to indicate that a specified heap table and its index are currently being reindexed, preventing re-entrant reindex operations.

## Definition


## Detailed Description
SetReindexProcessing is a static function that establishes the global reindexing state by setting the currently reindexed heap and index OIDs. This function enforces the non-re-entrant nature of reindexing operations by checking if a reindex is already in progress and raising an error if so. It also removes the index from the pending reindex list and records the current transaction nesting level for proper cleanup during transaction abort scenarios.

## Parameters / Member Variables
- : The OID of the heap table being reindexed
- : The OID of the index being reindexed

## Dependencies
- Functions called/Symbols referenced:
  - RemoveReindexPending
  - GetCurrentTransactionNestLevel
- Called from (representative examples):
  - reindex_index
  - SerializedReindexState

## Notes and Other Information
- This function enforces that reindexing operations are not re-entrant - attempting to start a reindex while another is in progress will result in an ERROR
- Both heapOid and indexOid must be valid OIDs (checked via Assert)
- The function automatically removes the index from any pending reindex state
- Sets the reindexing transaction nesting level to enable proper cleanup on transaction abort
- This is a static function within src/backend/catalog/index.c and is not exposed outside this module