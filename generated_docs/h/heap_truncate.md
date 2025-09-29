# heap_truncate

## Location
[src/backend/catalog/heap.c:3069-3109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L3069-L3109)

## Overview
heap_truncate is a non-transaction-safe function that deletes all data within specified relations, primarily used for ON COMMIT truncation of temporary tables.

## Definition
void heap_truncate(List *relids)

## Detailed Description
This function truncates multiple relations by completely removing all their data. It operates in two phases: first opening all specified relations with exclusive locks and checking for foreign key constraints, then truncating each relation individually. The function is explicitly marked as not transaction-safe and is now primarily used only for ON COMMIT truncation of temporary tables where transaction safety is not required.

The function differs from the transaction-safe implementation in commands/tablecmds.c by not providing rollback capabilities. It maintains exclusive locks on all relations until commit to prevent concurrent access during the truncation process.

## Parameters / Member Variables
- : List of relation OIDs to be truncated

## Dependencies
- Functions called/Symbols referenced:
  - lfirst_oid
  - [table_open](../t/table_open.md)
  - AccessExclusiveLock
  - [lappend](../l/lappend.md)
  - [heap_truncate_check_FKs](heap_truncate_check_FKs.md)
  - lfirst
  - [heap_truncate_one_rel](heap_truncate_one_rel.md)
  - [table_close](../t/table_close.md)
  - NoLock
- Called from (representative examples):
  - [PreCommit_on_commit_actions](../P/PreCommit_on_commit_actions.md)

## Notes and Other Information
- This function is NOT transaction-safe and cannot be rolled back
- Primarily used for ON COMMIT truncation of temporary tables
- A transaction-safe alternative exists in commands/tablecmds.c for regular TRUNCATE operations
- Maintains exclusive locks on all relations until transaction commit
- Performs foreign key constraint checking before truncation to prevent referential integrity violations
- Processes multiple relations in a single operation for efficiency

## Simplified Source

```c
void
heap_truncate(List *relids)
{
    List *relations = NIL;
    ListCell *cell;

    // Open all relations with exclusive locks
    foreach(cell, relids)
    {
        Oid rid = lfirst_oid(cell);
        Relation rel;

        rel = table_open(rid, AccessExclusiveLock);
        relations = lappend(relations, rel);
    }

    // Check foreign key constraints before truncation
    heap_truncate_check_FKs(relations, true);

    // Truncate each relation
    foreach(cell, relations)
    {
        Relation rel = lfirst(cell);

        // Perform the actual truncation
        heap_truncate_one_rel(rel);

        // Close relation but keep exclusive lock until commit
        table_close(rel, NoLock);
    }
}
```