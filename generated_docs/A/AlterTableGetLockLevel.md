# AlterTableGetLockLevel

## Location
src/backend/commands/tablecmds.c: 4473 - 4743

## Overview
AlterTableGetLockLevel determines the minimum lock level required for a list of ALTER TABLE subcommands, implementing PostgreSQL's lock level policy to ensure safe concurrent access during table alterations.

## Definition
```c
LOCKMODE AlterTableGetLockLevel(List *cmds)
```

## Detailed Description
AlterTableGetLockLevel is a critical function that analyzes a list of ALTER TABLE subcommands and determines the strongest lock level required across all commands. The function implements PostgreSQL's sophisticated locking policy that balances concurrency with data integrity and consistency requirements.

The function categorizes subcommands into different lock level groups based on their impact:
- **AccessExclusiveLock**: Required for operations that rewrite the heap, affect SELECT visibility, or modify table structure significantly
- **ShareRowExclusiveLock**: For operations affecting only write operations, like triggers and foreign key constraints
- **ShareUpdateExclusiveLock**: For operations affecting performance strategies but not semantic results
- **AccessShareLock**: For minimal operations like schema examination

The function must provide consistent results when called before and after parsing, as some subcommands may be transformed but never to a weaker lock level. It operates without table metadata access since it's called before table locking.

## Parameters / Member Variables
- `cmds`: A list of AlterTableCmd structures representing the subcommands to analyze

## Dependencies
- Functions called/Symbols referenced:
  - AlterTableGetRelOptionsLockLevel
  - Various AT_* subcommand type constants
  - Lock mode constants (AccessExclusiveLock, ShareRowExclusiveLock, etc.)
- Called from (representative examples):
  - AlterTableInternal
  - ProcessUtilitySlow

## Notes and Other Information
- Must work with MVCC catalog table reads
- Considers Hot Standby requirements - operations affecting SELECs on standbys need AccessExclusiveLock
- Accounts for pg_dump compatibility using AccessShareLock
- Returns the strongest lock mode required among all subcommands
- Some operations could theoretically use weaker locks but use stronger ones for consistency
- Special handling for constraint types (PRIMARY, UNIQUE, FOREIGN, etc.) with different lock requirements
- Partition operations have specific concurrent vs non-concurrent lock level differences