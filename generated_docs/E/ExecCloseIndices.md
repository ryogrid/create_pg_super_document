# ExecCloseIndices

## Location
[src/backend/executor/execIndexing.c:231-297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execIndexing.c#L231-L297)

## Overview
Closes all index relations that were previously opened by ExecOpenIndices, performing post-insertion cleanup and releasing locks acquired during index operations.

## Definition
```c
void ExecCloseIndices(ResultRelInfo *resultRelInfo)
```

## Detailed Description
The ExecCloseIndices function is the cleanup counterpart to ExecOpenIndices, responsible for properly closing index relations and releasing resources after index operations are complete. It is a critical part of PostgreSQL executor cleanup that ensures proper resource management and lock release.

The function performs the following operations:
1. Iterates through all opened index relations stored in the ResultRelInfo structure
2. For each valid index relation, calls index_insert_cleanup to allow the index access method to perform any necessary post-insertion cleanup
3. Closes each index relation using index_close, which releases the RowExclusiveLock that was acquired during ExecOpenIndices
4. Handles null index descriptors gracefully (though this should not normally occur)

The function includes a comment noting that IndexInfo arrays are not explicitly freed, as they are expected to be cleaned up automatically when the executor state is freed. This design follows PostgreSQL memory management patterns where executor-related memory is managed through memory contexts.

## Parameters / Member Variables
- `resultRelInfo`: Pointer to ResultRelInfo structure containing the opened index information that needs to be closed. This should be the same structure that was passed to ExecOpenIndices.

## Dependencies
- Functions called/Symbols referenced:
  - [index_insert_cleanup](../i/index_insert_cleanup.md): Allows index access methods to perform post-insertion cleanup operations
  - [index_close](../i/index_close.md): Closes the index relation and releases the RowExclusiveLock
- Called from (representative examples):
  - [ExecCloseResultRelations](ExecCloseResultRelations.md): Part of general executor cleanup when closing result relations
  - [CatalogCloseIndexes](../C/CatalogCloseIndexes.md): Closes system catalog indices after catalog operations
  - [apply_handle_insert](../a/apply_handle_insert.md)/update/delete: Cleanup in logical replication worker operations
  - [ExecCleanupTupleRouting](ExecCleanupTupleRouting.md): Cleanup when finishing partition tuple routing

## Notes and Other Information
- Must be called for every ResultRelInfo that had ExecOpenIndices called on it to prevent resource leaks
- The function gracefully handles null index descriptors, though such cases should not normally occur
- IndexInfo arrays are not explicitly freed - they rely on executor memory context cleanup
- The function releases RowExclusiveLock that was acquired during ExecOpenIndices
- Part of the executor resource cleanup protocol and should be called during executor shutdown
- Does not return any error status - [cleanup](../c/cleanup.md) operations are expected to succeed