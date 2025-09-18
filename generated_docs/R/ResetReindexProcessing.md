# ResetReindexProcessing

## Location
[src/backend/catalog/index.c:4109-4122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L4109-L4122)

## Overview
Clears the global reindexing state by resetting the currently reindexed heap and index OIDs to invalid values, indicating that no reindex operation is currently in progress.

## Definition


## Detailed Description
ResetReindexProcessing is a static function that clears the global reindexing state by setting both currentlyReindexedHeap and currentlyReindexedIndex to InvalidOid. This function is called when a reindex operation completes or needs to be cleaned up. Notably, it does not reset the reindexingNestLevel variable, which remains set until the end of the current transaction or subtransaction to enable proper cleanup during transaction abort scenarios.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - None (only uses global variables and constants)
- Called from (representative examples):
  - [reindex_index](../r/reindex_index.md)
  - [IndexCheckExclusion](../I/IndexCheckExclusion.md)
  - SerializedReindexState

## Notes and Other Information
- This function only resets the heap and index OID markers, leaving reindexingNestLevel intact
- The reindexingNestLevel remains set until the end of the transaction/subtransaction for proper abort handling
- This is a static function within src/backend/catalog/index.c and is not exposed outside this module
- Called both during normal completion of reindex operations and during cleanup scenarios