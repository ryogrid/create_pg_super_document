# index_concurrently_build

## Location
[src/backend/catalog/index.c:1482-1548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L1482-L1548)

## Overview
index_concurrently_build performs the actual data building phase for a concurrent index operation, constructing the index data while allowing concurrent table access.

## Definition

```c
struct, since it was lost in the
	 * commit of the transaction where this concurrent index was created at
	 * the catalog level.
	 */
	indexInfo = BuildIndexInfo(indexRelation);
```
## Detailed Description
This function performs the data building phase of concurrent index creation. It opens the heap relation and index relation, switches to the table owner's user context for security, rebuilds the IndexInfo structure (which was lost after the catalog creation transaction committed), and then calls index_build to construct the actual index data. After completion, it marks the index as ready for inserts.

The function operates under security restrictions and manages GUC variable changes locally. It maintains locks throughout the operation to prevent schema changes while allowing concurrent data access. The index is marked with ii_Concurrent=true to indicate concurrent building mode, which affects how the index building process handles concurrent updates.

## Parameters / Member Variables
- : Object identifier of the table relation to build the index on
- : Object identifier of the index relation to build

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (to open the heap relation)
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)/SetUserIdAndSecContext (for user context management)
  - [NewGUCNestLevel](../N/NewGUCNestLevel.md)/AtEOXact_GUC (for GUC variable management)
  - [RestrictSearchPath](../R/RestrictSearchPath.md) (for security restrictions)
  - [index_open](index_open.md) (to open the index relation)
  - [BuildIndexInfo](../B/BuildIndexInfo.md) (to reconstruct IndexInfo structure)
  - [index_build](index_build.md) (to perform the actual index building)
  - [index_close](index_close.md) (for cleanup)
  - [index_set_state_flags](index_set_state_flags.md) (to mark index as ready)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md) (during concurrent index creation)

## Notes and Other Information
- This is a void function that does not return a value
- The function assumes an active snapshot is set (assertion check)
- Uses ShareUpdateExclusiveLock on the heap relation to prevent schema changes
- Switches to table owner's userid for security during index function execution
- Rebuilds IndexInfo since it's lost after the catalog creation transaction commits
- Sets ii_Concurrent=true and ii_BrokenHotChain=false for concurrent building
- Marks the index as ready for inserts using INDEX_CREATE_SET_READY flag
- Maintains locks until transaction end but closes relations
- Manages GUC variables and security context properly during execution
- Located at src/backend/catalog/index.c:1482-1548