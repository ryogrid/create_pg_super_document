# ATPrepCmd

## Location
[src/backend/commands/tablecmds.c:4779-5157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L4779-L5157)

## Overview
ATPrepCmd serves as the traffic controller for ALTER TABLE Phase 1 operations, handling permissions, recursion, and command-specific preparation for each ALTER TABLE subcommand type.

## Definition
```c
static void ATPrepCmd(List **wqueue, Relation rel, AlterTableCmd *cmd, bool recurse, bool recursing, LOCKMODE lockmode, AlterTableUtilityContext *context)
```

## Detailed Description
ATPrepCmd is the central dispatcher for the first phase of ALTER TABLE processing, responsible for analyzing each subcommand and preparing it for execution. The function implements a comprehensive switch statement that handles all ALTER TABLE subcommand types, performing permissions checks, recursion setup, and command-specific preparation.

Key responsibilities include:
- **Permission Validation**: Uses ATSimplePermissions to verify appropriate access rights for each operation type
- **Recursion Management**: Determines when and how to apply changes to child tables through inheritance hierarchies
- **Work Queue Management**: Organizes commands into appropriate execution passes for Phase 2
- **Command Transformation**: Some commands undergo parse transformation via ATParseTransformCmd
- **Dependency Checking**: Validates constraints and dependencies before execution

The function categorizes operations into execution passes (AT_PASS_*) that determine the order of execution in later phases. It also handles special cases like partition detach validation and persistence change restrictions.

## Parameters / Member Variables
- `wqueue`: Pointer to the work queue list for organizing subcommands by execution pass
- `rel`: The relation being altered (must have appropriate lock already acquired)
- `cmd`: The specific ALTER TABLE subcommand to prepare
- `recurse`: Whether to apply changes recursively to child tables
- `recursing`: Whether this call is part of a recursive operation
- `lockmode`: The lock mode being used for this operation
- `context`: Utility context containing additional state and configuration

## Dependencies
- Functions called/Symbols referenced:
  - [ATGetQueueEntry](ATGetQueueEntry.md)
  - [ATSimplePermissions](ATSimplePermissions.md)
  - [ATSimpleRecursion](ATSimpleRecursion.md)
  - [ATPrepAddColumn](ATPrepAddColumn.md)
  - [ATPrepDropColumn](ATPrepDropColumn.md)
  - [ATPrepSetNotNull](ATPrepSetNotNull.md)
  - [ATPrepAlterColumnType](ATPrepAlterColumnType.md)
  - Various AT_* subcommand constants
  - Multiple AT_PASS_* execution pass constants
- Called from (representative examples):
  - [ATController](ATController.md)
  - [ATSimpleRecursion](ATSimpleRecursion.md)
  - [ATTypedTableRecursion](ATTypedTableRecursion.md)

## Notes and Other Information
- Static function scope limits access to internal ALTER TABLE implementation
- Includes special handling for partitions with pending detach operations
- Commands are copied using copyObject to avoid conflicts during child table processing
- Some operations require table rewrites and are marked accordingly
- Persistence changes (LOGGED/UNLOGGED) have restrictions on multiple modifications
- The function determines execution passes that control the order of operations in subsequent phases
- [Complex](../C/Complex.md) operations like ALTER COLUMN TYPE undergo parse transformation
- Recursion behavior varies by command type - some recurse during preparation, others during execution

## Simplified Source

```c
static void
ATPrepCmd(List **wqueue, Relation rel, AlterTableCmd *cmd,
          bool recurse, bool recursing, LOCKMODE lockmode,
          AlterTableUtilityContext *context)
{
    AlteredTableInfo *tab;
    AlterTablePass pass = AT_PASS_UNSET;

    // Find or create work queue entry for this table
    tab = ATGetQueueEntry(wqueue, rel);

    // Check for pending partition detach operations
    if (rel->rd_rel->relispartition &&
        cmd->subtype != AT_DetachPartitionFinalize &&
        PartitionHasPendingDetach(RelationGetRelid(rel)))
        ereport(ERROR, /* partition detach error */);

    // Copy command to avoid conflicts in child table processing
    cmd = copyObject(cmd);

    // Process command based on subtype
    switch (cmd->subtype)
    {
        case AT_AddColumn:
            ATSimplePermissions(cmd->subtype, rel, ATT_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE);
            ATPrepAddColumn(wqueue, rel, recurse, recursing, false, cmd, lockmode, context);
            pass = AT_PASS_ADD_COL;
            break;

        case AT_DropColumn:
            ATSimplePermissions(cmd->subtype, rel, ATT_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE);
            ATPrepDropColumn(wqueue, rel, recurse, recursing, cmd, lockmode, context);
            pass = AT_PASS_DROP;
            break;

        case AT_AlterColumnType:
            ATSimplePermissions(cmd->subtype, rel, ATT_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE);
            cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, recurse, lockmode, AT_PASS_UNSET, context);
            ATPrepAlterColumnType(wqueue, tab, rel, recurse, recursing, cmd, lockmode, context);
            pass = AT_PASS_ALTER_TYPE;
            break;

        case AT_AddConstraint:
            ATSimplePermissions(cmd->subtype, rel, ATT_TABLE | ATT_FOREIGN_TABLE);
            if (recurse) cmd->recurse = true;
            pass = AT_PASS_ADD_CONSTR;
            break;

        case AT_DropConstraint:
            ATSimplePermissions(cmd->subtype, rel, ATT_TABLE | ATT_FOREIGN_TABLE);
            ATCheckPartitionsNotInUse(rel, lockmode);
            if (recurse) cmd->recurse = true;
            pass = AT_PASS_DROP;
            break;

        // Many more cases for different ALTER TABLE operations...
        // Each checks permissions, sets up recursion, and assigns execution pass

        default:
            elog(ERROR, "unrecognized alter table type: %d", (int) cmd->subtype);
            pass = AT_PASS_UNSET;
            break;
    }

    Assert(pass > AT_PASS_UNSET);

    // Add the subcommand to appropriate execution pass list
    tab->subcmds[pass] = lappend(tab->subcmds[pass], cmd);
}
```