# LockStmt

## Location
[src/include/nodes/parsenodes.h:3942-3948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3942-L3948)

## Overview
LockStmt represents a LOCK TABLE statement in PostgreSQL's parse tree, which is used to explicitly acquire table locks on one or more relations.

## Definition
```c
typedef struct LockStmt
{
    NodeTag     type;
    List       *relations;    /* relations to lock */
    int         mode;         /* lock mode */
    bool        nowait;       /* no wait mode */
} LockStmt;
```

## Detailed Description
LockStmt is a parse tree node that represents the LOCK TABLE SQL command. The LOCK TABLE statement allows users to explicitly acquire locks on tables before performing operations that require specific locking semantics. This is particularly useful for preventing deadlocks in complex transactions or ensuring consistent access patterns.

The statement can lock multiple tables in a single command and supports different lock modes ranging from least restrictive (ACCESS SHARE) to most restrictive (ACCESS EXCLUSIVE). The NOWAIT option allows non-blocking lock acquisition, causing the statement to fail immediately if the lock cannot be obtained.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a LockStmt node in the parse tree
- `relations`: List of RangeVar structures representing the tables to lock
- `mode`: Integer representing the lock mode (maps to LOCKMODE constants like AccessShareLock, RowShareLock, etc.)
- `nowait`: Boolean flag indicating whether to wait for locks or fail immediately if unable to acquire

## Dependencies
- Functions called/Symbols referenced:
  - [List](List.md) (PostgreSQL's list data structure)
  - [RangeVar](../R/RangeVar.md) (for table references)
  
- Called from (representative examples):
  - [LockTableCommand](LockTableCommand.md) (main execution function in lockcmds.c:41)
  - [PlannedStmtRequiresSnapshot](../P/PlannedStmtRequiresSnapshot.md) (snapshot requirement check in pquery.c:1740)
  - [ClassifyUtilityCommandAsReadOnly](../C/ClassifyUtilityCommandAsReadOnly.md) (read-only classification in utility.c:343)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility command processor in utility.c:934)

## Notes and Other Information
- The LOCK TABLE command is primarily used for explicit lock management in complex transactions
- Supports locking of views, which recursively locks the underlying base tables
- The nowait option prevents blocking and is useful in applications that need immediate feedback
- Lock modes range from ACCESS SHARE (least restrictive) to ACCESS EXCLUSIVE (most restrictive)
- The actual lock acquisition and permission checking logic is handled in src/backend/commands/lockcmds.c
- Supports inheritance hierarchies - can lock parent and child tables together