# AlterTableGetLockLevel

## Location
[src/backend/commands/tablecmds.c:4473-4743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L4473-L4743)

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
  - [AlterTableGetRelOptionsLockLevel](AlterTableGetRelOptionsLockLevel.md)
  - Various AT_* subcommand type constants
  - Lock mode constants (AccessExclusiveLock, ShareRowExclusiveLock, etc.)
- Called from (representative examples):
  - [AlterTableInternal](AlterTableInternal.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Must work with MVCC catalog table reads
- Considers Hot Standby requirements - operations affecting SELECs on standbys need AccessExclusiveLock
- Accounts for pg_dump compatibility using AccessShareLock
- Returns the strongest lock mode required among all subcommands
- Some operations could theoretically use weaker locks but use stronger ones for consistency
- Special handling for constraint types (PRIMARY, UNIQUE, FOREIGN, etc.) with different lock requirements
- Partition operations have specific concurrent vs non-concurrent lock level differences

## Simplified Source

```c
LOCKMODE AlterTableGetLockLevel(List *cmds)
{
    ListCell   *lcmd;
    LOCKMODE    lockmode = ShareUpdateExclusiveLock;  // Default lock level

    // Examine each subcommand and determine required lock level
    foreach(lcmd, cmds)
    {
        AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);
        LOCKMODE cmd_lockmode = AccessExclusiveLock;  // Default for safety

        switch (cmd->subtype)
        {
            // Operations that rewrite the heap - need strongest lock
            case AT_AddColumn:
            case AT_SetAccessMethod:
            case AT_SetTableSpace:
            case AT_AlterColumnType:
            case AT_SetStorage:  // May add toast tables
                cmd_lockmode = AccessExclusiveLock;
                break;

            // Operations affecting SELECT visibility
            case AT_DropConstraint:
            case AT_DropNotNull:
            case AT_DropColumn:
            case AT_ChangeOwner:
            case AT_EnableRule:
            case AT_DisableRule:
                cmd_lockmode = AccessExclusiveLock;
                break;

            // Operations affecting only writes
            case AT_EnableTrig:
            case AT_DisableTrig:
                cmd_lockmode = ShareRowExclusiveLock;
                break;

            // Constraint operations - depend on constraint type
            case AT_AddConstraint:
                if (IsA(cmd->def, Constraint))
                {
                    Constraint *con = (Constraint *) cmd->def;
                    if (con->contype == CONSTR_FOREIGN)
                        cmd_lockmode = ShareRowExclusiveLock;
                    else
                        cmd_lockmode = AccessExclusiveLock;
                }
                break;

            // Performance/maintenance operations
            case AT_SetStatistics:
            case AT_ClusterOn:
            case AT_ValidateConstraint:
                cmd_lockmode = ShareUpdateExclusiveLock;
                break;

            // Minimal operations
            case AT_CheckNotNull:
                cmd_lockmode = AccessShareLock;
                break;

            default:
                cmd_lockmode = AccessExclusiveLock;
                break;
        }

        // Take the strongest lock mode required
        if (cmd_lockmode > lockmode)
            lockmode = cmd_lockmode;
    }

    return lockmode;
}
```