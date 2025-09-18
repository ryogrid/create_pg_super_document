# RemoveReindexPending

## Location
[src/backend/catalog/index.c:4139-4151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L4139-L4151)

## Overview
Removes a specified index OID from the global list of indexes pending reindex, with safety checks to prevent modification during parallel operations.

## Definition


## Detailed Description
RemoveReindexPending is a static function that removes a specific index from the pendingReindexedIndexes list. The function includes an important safety check to prevent modification of the reindex state during parallel operations, which could lead to race conditions. It uses the PostgreSQL list utility function list_delete_oid to safely remove the specified index OID from the pending list.

## Parameters / Member Variables
- : The OID of the index to remove from the pending reindex list

## Dependencies
- Functions called/Symbols referenced:
  - IsInParallelMode
  - [list_delete_oid](../l/list_delete_oid.md)
- Called from (representative examples):
  - [SetReindexProcessing](../S/SetReindexProcessing.md)
  - [reindex_relation](../r/reindex_relation.md)
  - SerializedReindexState

## Notes and Other Information
- Prevents modification of reindex state during parallel operations to avoid race conditions
- Uses list_delete_oid which safely handles cases where the OID is not in the list
- Typically called when an index transitions from "pending" to "processing" state
- This is a static function within src/backend/catalog/index.c and is not exposed outside this module
- The function modifies the global pendingReindexedIndexes list directly