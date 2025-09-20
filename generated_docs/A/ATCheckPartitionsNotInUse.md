# ATCheckPartitionsNotInUse

## Location
[src/backend/commands/tablecmds.c:6663-6692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L6663-L6692)

## Overview
A specialized validation function that ensures all partitions of a partitioned table are safe for ALTER TABLE operations by checking that they are not currently in use by other transactions.

## Definition

```c
static void
ATCheckPartitionsNotInUse(Relation rel, LOCKMODE lockmode)
```
## Detailed Description
ATCheckPartitionsNotInUse performs safety validation specifically for partitioned tables by examining all partitions in the partition hierarchy. The function uses find_all_inheritors to discover all partitions, then opens each partition with the specified lock mode and runs CheckAlterTableIsSafe to ensure the partition is not in use by concurrent transactions. This validation is essential for ALTER TABLE operations that could be unsafe if performed while partitions are being accessed. The function is a no-op for non-partitioned tables and specifically ignores legacy inheritance relationships, focusing only on modern partitioned table structures.

## Parameters / Member Variables
- : The Relation structure representing the partitioned table whose partitions need validation
- : The lock mode to acquire on each partition during the safety check

## Dependencies
- Functions called/Symbols referenced:
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - table_open
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md)
  - table_close
  - [list_free](../l/list_free.md)
  - RelationGetRelid
  - for_each_from (macro)
- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md) (for operations requiring partition safety validation)

## Notes and Other Information
- Only operates on partitioned tables (RELKIND_PARTITIONED_TABLE), ignoring other relation types
- Distinguishes between modern partitioned tables and legacy inheritance hierarchies
- Skips the parent relation itself when iterating through the inheritance list (starts from index 1)
- Uses table_open/table_close instead of relation_open/relation_close for partition access
- Relies on find_all_inheritors for proper locking, using NoLock for subsequent table operations
- Memory management includes explicit list_free call to clean up the inheritance list
- Designed to prevent unsafe ALTER TABLE operations on busy partitioned table systems