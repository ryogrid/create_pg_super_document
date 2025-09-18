# SetReindexPending

## Location
src/backend/catalog/index.c: 4123 - 4138

## Overview
Marks a list of indexes as pending reindex by storing them in the global pendingReindexedIndexes list, with safety checks to prevent re-entrant operations and parallel execution conflicts.

## Definition


## Detailed Description
SetReindexPending is a static function that establishes a list of indexes that are pending reindex by copying the provided index list to the global pendingReindexedIndexes variable. The function includes important safety checks: it prevents re-entrant reindexing operations by checking if there are already pending indexes, and it prevents modification of reindex state during parallel operations. The function also records the current transaction nesting level for proper cleanup during transaction abort scenarios.

## Parameters / Member Variables
- : A List of index OIDs that should be marked as pending reindex

## Dependencies
- Functions called/Symbols referenced:
  - IsInParallelMode
  - list_copy
  - GetCurrentTransactionNestLevel
- Called from (representative examples):
  - reindex_relation
  - SerializedReindexState

## Notes and Other Information
- Enforces non-re-entrant reindexing by checking if pendingReindexedIndexes is already set
- Prevents reindex state modification during parallel operations to avoid race conditions
- Makes a copy of the input list rather than storing a reference, assuming the current memory context remains valid
- Sets the reindexing transaction nesting level for proper cleanup on transaction abort
- This is a static function within src/backend/catalog/index.c and is not exposed outside this module