# ATExecDropInherit

## Location
[src/backend/commands/tablecmds.c:16141-16182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L16141-L16182)

## Overview
Executes the ALTER TABLE NO INHERIT command to remove inheritance relationship between a child table and its parent table.

## Definition

```c
static ObjectAddress
ATExecDropInherit(Relation rel, RangeVar *parent, LOCKMODE lockmode)
```
## Detailed Description
ATExecDropInherit implements the core logic for the ALTER TABLE NO INHERIT SQL command. This function removes an inheritance relationship between a child table (rel) and a specified parent table. The function validates that the child table is not a partition (as partitions cannot have their inheritance changed), opens the parent relation with appropriate locking, and delegates the actual inheritance removal work to the RemoveInheritance function. It returns an ObjectAddress representing the parent relation that is no longer inherited from.

## Parameters / Member Variables
- : The child relation from which inheritance is being removed
- : RangeVar structure identifying the parent table to be removed from inheritance
- : Lock mode parameter (though not directly used in the function body)

## Dependencies
- Functions called/Symbols referenced:
  - table_openrv
  - [RemoveInheritance](../R/RemoveInheritance.md)
  - ObjectAddressSet
  - table_close
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - child_dependency_type

## Notes and Other Information
- The function prevents inheritance changes on partitioned tables by raising an error if the child relation is a partition
- Uses AccessShareLock on the parent table, which is deemed sufficient since DROP TABLE doesn't lock parent tables
- Does not check ownership of the parent table, assuming ownership of the child table provides sufficient rights
- Keeps the lock on the parent relation until transaction commit for consistency
- Returns ObjectAddress of the parent relation that was removed from inheritance