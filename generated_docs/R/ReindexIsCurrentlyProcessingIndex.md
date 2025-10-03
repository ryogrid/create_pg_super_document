# ReindexIsCurrentlyProcessingIndex

## Location
[src/backend/catalog/index.c:4068-4078](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L4068-L4078)

## Overview
The  function checks whether a specific index is currently being actively reindexed by comparing against a global tracking variable.

## Definition

```c
static bool
ReindexIsCurrentlyProcessingIndex(Oid indexOid)
```
## Detailed Description
This static function provides a boolean check to determine if an index identified by its OID is currently being processed during an active reindex operation. It compares the provided index OID against the global variable  that tracks which index is currently undergoing reconstruction. This mechanism is part of PostgreSQL's reindex coordination system that prevents operations on indexes that are in an inconsistent state during rebuilding.

The function is used internally within the index management system to coordinate various index operations and ensure that constraints and other index-dependent operations behave correctly when an index is being rebuilt. It's particularly important for exclusion constraint checking and serialized reindex state management.

## Parameters / Member Variables
- `indexOid`: Object identifier of the index to check for active reindexing
## Dependencies
- Functions called/Symbols referenced:
  - currentlyReindexedIndex: Global variable tracking the currently reindexed index
- Called from (representative examples):
  - [IndexCheckExclusion](../I/IndexCheckExclusion.md): Exclusion constraint validation during index operations
  - SerializedReindexState: State management for reindex coordination

## Notes and Other Information
- Returns true if the specified index is currently being reindexed, false otherwise
- Declared as static, limiting its scope to the index.c compilation unit
- Part of the internal reindex coordination mechanism alongside ReindexIsProcessingHeap
- Critical for preventing operations on inconsistent indexes during rebuilding
- Used by constraint checking functions to handle special cases during reindexing
- Ensures proper behavior when indexes are temporarily in an invalid state

## Simplified Source

```c
static bool ReindexIsCurrentlyProcessingIndex(Oid indexOid) {
    // Check if this index is currently being reindexed
    return indexOid == currentlyReindexedIndex;
}
```