# logicalrep_rel_close

## Location
[src/backend/replication/logical/relation.c:473-491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/relation.c#L473-L491)

## Overview
Closes a previously opened logical replication relation by releasing the table lock and clearing the local relation reference to prevent resource leaks.

## Definition
```c
void logicalrep_rel_close(LogicalRepRelMapEntry *rel, LOCKMODE lockmode)
```

## Detailed Description
This is a simple cleanup function that properly closes a logical replication relation that was previously opened with logicalrep_rel_open(). It serves as the counterpart to the open operation and ensures proper resource management by releasing the table lock and clearing the local relation pointer.

The function performs two critical operations: it calls table_close() to release the lock on the local relation and decrements the relation cache reference count, then sets the localrel pointer to NULL to indicate that the relation is no longer open and prevent accidental access to a closed relation.

This function is essential for preventing relation cache reference leaks in logical replication operations, especially in error handling paths and after completing operations on specific relations.

## Parameters / Member Variables
- `rel`: Pointer to LogicalRepRelMapEntry containing the logical replication relation mapping that needs to be closed
- `lockmode`: LOCKMODE specifying the type of lock to release, must match the lock mode used when opening the relation

## Dependencies
- Functions called/Symbols referenced:
  - [table_close](../t/table_close.md): PostgreSQL core function to close a table relation and release its lock
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md): Structure type representing the mapping between local and remote relations
- Called from (representative examples):
  - [copy_table](../c/copy_table.md): Table synchronization cleanup operations
  - [apply_handle_insert](../a/apply_handle_insert.md): Cleanup after processing INSERT operations
  - [apply_handle_update](../a/apply_handle_update.md): Cleanup after processing UPDATE operations  
  - [apply_handle_delete](../a/apply_handle_delete.md): Cleanup after processing DELETE operations
  - [apply_handle_truncate](../a/apply_handle_truncate.md): Cleanup after processing TRUNCATE operations

## Notes and Other Information
- This function should always be called to match every successful logicalrep_rel_open() call
- The lockmode parameter must match the mode used when opening the relation to ensure proper lock management
- Setting localrel to NULL prevents use-after-free bugs and makes debugging easier
- This function is typically called in both success and error paths to ensure proper cleanup
- Does not invalidate the LogicalRepRelMapEntry itself - only closes the local relation reference
- The relation map entry remains valid and can be reopened later if needed

## Simplified Source

```c
void logicalrep_rel_close(LogicalRepRelMapEntry *rel, LOCKMODE lockmode) {
    // Close the local relation and release its lock
    table_close(rel->localrel, lockmode);

    // Clear the relation pointer to prevent accidental access
    rel->localrel = NULL;
}
```