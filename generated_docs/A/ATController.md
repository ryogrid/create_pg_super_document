# ATController

## Location
[src/backend/commands/tablecmds.c:4744-4778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L4744-L4778)

## Overview
ATController serves as the top-level controller for ALTER TABLE operations, orchestrating the three-phase execution process that ensures safe and consistent table alterations.

## Definition
```c
static void ATController(AlterTableStmt *parsetree, Relation rel, List *cmds, bool recurse, LOCKMODE lockmode, AlterTableUtilityContext *context)
```

## Detailed Description
ATController implements PostgreSQL's sophisticated three-phase approach to ALTER TABLE operations, ensuring that complex table alterations are performed safely and consistently. The function acts as the central coordinator that manages the entire ALTER TABLE workflow.

The three phases are:
1. **Phase 1 (Preparation)**: Preliminary examination of commands and creation of a work queue using ATPrepCmd
2. **Phase 2 (Catalog Updates)**: System catalog updates via ATRewriteCatalogs
3. **Phase 3 (Table Rewriting)**: Table scanning/rewriting and execution of after-statements through ATRewriteTables

The function maintains the relation open during the preparation phase but closes it before the catalog update phase, retaining the lock until commit to ensure consistency. The parsetree parameter is preserved to allow event trigger access when needed.

## Parameters / Member Variables
- `parsetree`: The original ALTER TABLE statement, passed to event triggers when requested
- `rel`: The relation being altered (will be closed after Phase 1)
- `cmds`: List of ALTER TABLE subcommands to execute
- `recurse`: Boolean indicating whether to apply changes recursively to child tables
- `lockmode`: The lock mode to use throughout the operation
- `context`: Utility context containing additional operation state and configuration

## Dependencies
- Functions called/Symbols referenced:
  - [ATPrepCmd](ATPrepCmd.md)
  - [relation_close](../r/relation_close.md)
  - [ATRewriteCatalogs](ATRewriteCatalogs.md)
  - [ATRewriteTables](ATRewriteTables.md)
- Called from (representative examples):
  - [AlterTable](AlterTable.md)
  - [AlterTableInternal](AlterTableInternal.md)

## Notes and Other Information
- Uses a static function scope, indicating it's an internal implementation detail
- Maintains strict phase separation to ensure transaction safety
- The work queue (wqueue) serves as the communication mechanism between phases
- [Relation](../R/Relation.md) is closed after Phase 1 but lock is retained until commit
- Event trigger support is maintained through the parsetree parameter
- The three-phase approach allows for proper dependency handling and rollback capabilities

## Simplified Source

```c
static void ATController(AlterTableStmt *parsetree, Relation rel, List *cmds,
                        bool recurse, LOCKMODE lockmode, AlterTableUtilityContext *context) {
    List *work_queue = NIL;

    // Phase 1: Build work queue from commands
    foreach(cmd, cmds) {
        ATPrepCmd(&work_queue, rel, cmd, recurse, false, lockmode, context);
    }

    // Close relation but keep lock until commit
    relation_close(rel, NoLock);

    // Phase 2: Update system catalogs
    ATRewriteCatalogs(&work_queue, lockmode, context);

    // Phase 3: Rewrite tables and run after-statements
    ATRewriteTables(parsetree, &work_queue, lockmode, context);
}
```