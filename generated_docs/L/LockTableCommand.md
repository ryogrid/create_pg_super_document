# LockTableCommand

## Location
[src/backend/commands/lockcmds.c:41-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/lockcmds.c#L41-L70)

## Overview
Implements the PostgreSQL LOCK TABLE command, processing a list of relations to be locked with a specified lock mode and handling inheritance scenarios appropriately.

## Definition

```c
void
LockTableCommand(LockStmt *lockstmt)
```
## Detailed Description
LockTableCommand is the main entry point for executing LOCK TABLE statements in PostgreSQL. It iterates through each relation specified in the LOCK statement and acquires the requested lock mode on them. The function handles both regular tables and views differently - for views it calls LockViewRecurse to lock underlying tables, while for regular tables with inheritance it calls LockTableRecurse to lock child tables as well. The function respects the NOWAIT option when specified, avoiding blocking on unavailable locks.

## Parameters / Member Variables
- : Pointer to LockStmt structure containing the lock statement details including the list of relations to lock, lock mode, NOWAIT flag, and inheritance settings

## Dependencies
- Functions called/Symbols referenced:
  - LockStmt (structure type)
  - [RangeVar](../R/RangeVar.md) (structure type)
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - RVR_NOWAIT
  - [RangeVarCallbackForLockTable](../R/RangeVarCallbackForLockTable.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - RELKIND_VIEW
  - [LockViewRecurse](LockViewRecurse.md)
  - [LockTableRecurse](LockTableRecurse.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- The function processes each relation individually, allowing for mixed relation types in a single LOCK statement
- Special handling is provided for views, which require locking their underlying base tables rather than the view itself
- Inheritance is handled through the recurse flag, automatically including child tables when appropriate
- The NOWAIT option is passed through to underlying lock acquisition functions to maintain consistent behavior