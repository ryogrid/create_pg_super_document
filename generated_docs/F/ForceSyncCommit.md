# ForceSyncCommit

## Location
[src/backend/access/transam/xact.c:1149-1163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1149-L1163)

## Overview
ForceSyncCommit is an interface function that allows commands to force a synchronous commit of the current top-level transaction, ensuring durability by writing transaction data to disk before returning.

## Definition

```c
void
ForceSyncCommit(void)
```
## Detailed Description
ForceSyncCommit provides a mechanism for critical database operations to ensure that transaction commits are written synchronously to persistent storage rather than being buffered. This function sets the  flag to true, which affects the behavior of the current top-level transaction's commit process.

The function is designed for use with commands that require guaranteed durability, such as database creation, database dropping, and tablespace operations. It's important to note that two-phase commit does not persist and restore this variable, but since all callers are expected to use , this limitation has no practical consequences.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - forceSyncCommit (global variable)
- Called from (representative examples):
  - [createdb](../c/createdb.md) (src/backend/commands/dbcommands.c:1526)
  - [dropdb](../d/dropdb.md) (src/backend/commands/dbcommands.c:1855)
  - [movedb](../m/movedb.md) (src/backend/commands/dbcommands.c:2224)
  - [CreateTableSpace](../C/CreateTableSpace.md) (src/backend/commands/tablespace.c:379)
  - [DropTableSpace](../D/DropTableSpace.md) (src/backend/commands/tablespace.c:553)

## Notes and Other Information
- This function is typically used in conjunction with  to ensure proper transaction handling
- The forceSyncCommit flag affects only the current top-level transaction
- Two-phase commit operations do not preserve the forceSyncCommit state across transaction boundaries
- Critical for operations that cannot tolerate data loss, such as DDL operations on databases and tablespaces