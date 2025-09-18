# ExecuteTruncate

## Location
src/backend/commands/tablecmds.c: 1791 - 1914

## Overview
ExecuteTruncate executes a TRUNCATE command for one or more relations, handling multi-relation truncation with proper permission checks, lock acquisition, and cascade/restrict behavior for foreign key constraints.

## Definition
```c
void ExecuteTruncate(TruncateStmt *stmt)
```

## Detailed Description
This function implements the main logic for the TRUNCATE command, supporting both single and multiple table truncation operations. The execution follows a carefully orchestrated process:

1. **Relation Opening and Locking**: Opens each specified relation with AccessExclusiveLock to prevent concurrent access during truncation
2. **Permission and Validity Checks**: Validates that each relation can be truncated through callback functions and additional checks
3. **Inheritance Handling**: When inheritance is specified, automatically includes all child tables while handling special cases like temporary tables from other backends
4. **Logical Decoding Support**: Tracks relations that need to be logged for logical replication
5. **Partitioned Table Validation**: Prevents truncation of only the parent of a partitioned table without including partitions
6. **Delegation**: Calls ExecuteTruncateGuts to perform the actual truncation work
7. **Cleanup**: Properly closes all opened relations

The function supports PostgreSQL's inheritance hierarchy by recursively processing child tables when the INHERIT option is specified, while carefully handling edge cases like temporary tables from other database sessions.

## Parameters / Member Variables
- `stmt`: TruncateStmt structure containing the parsed TRUNCATE command details including:
  - relations: List of tables to truncate
  - behavior: CASCADE or RESTRICT behavior for foreign keys  
  - restart_seqs: Whether to restart sequences owned by the truncated tables

## Dependencies
- Functions called/Symbols referenced:
  - RangeVarGetRelidExtended
  - RangeVarCallbackForTruncate
  - table_open
  - truncate_check_activity
  - RelationIsLogicallyLogged
  - find_all_inheritors
  - truncate_check_rel
  - ExecuteTruncateGuts
  - table_close
- Called from (representative examples):
  - standard_ProcessUtility

## Notes and Other Information
- This function only handles the setup and validation phase of truncation - the actual data deletion is delegated to ExecuteTruncateGuts
- Foreign table truncation checks are deferred until the actual truncation phase when foreign data sources are accessed
- The function prevents truncation of partitioned tables without their partitions using the ONLY keyword
- Temporary tables from other backends are silently skipped during inheritance processing to avoid buffering conflicts
- All relations remain locked with AccessExclusiveLock until the entire operation completes to ensure consistency
- Logical decoding support ensures that truncation operations can be properly replicated when needed