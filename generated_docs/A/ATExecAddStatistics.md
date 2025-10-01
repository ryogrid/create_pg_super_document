# ATExecAddStatistics

## Location
[src/backend/commands/tablecmds.c:9242-9262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L9242-L9262)

## Overview
ATExecAddStatistics implements the internal creation of extended statistics objects during ALTER TABLE operations, specifically for rebuilding statistics after column type changes.

## Definition

```c
static ObjectAddress
ATExecAddStatistics(AlteredTableInfo *tab, Relation rel,
					CreateStatsStmt *stmt, bool is_rebuild, LOCKMODE lockmode)
```
## Detailed Description
This function serves as an internal mechanism for creating extended statistics objects within the ALTER TABLE infrastructure. Unlike other ALTER TABLE operations that correspond to explicit SQL commands, this function is used internally to add AT_ReAddStatistics subcommands that rebuild extended statistics after table column type changes. The function acts as a simple wrapper around CreateStatistics, providing the ALTER TABLE context while delegating the actual statistics creation to the standard statistics creation infrastructure.

The function is part of PostgreSQL's extended statistics system, which allows for multivariate statistics collection on groups of columns to improve query planning.

## Parameters / Member Variables
- `tab`: Information about the table being altered (not directly used in current implementation)
- `rel`: The relation (table) for which statistics are being created (not directly used in current implementation)
- `stmt`: The create statistics statement containing the statistics definition
- `is_rebuild`: Flag indicating whether this is rebuilding existing statistics (not directly used in current implementation)
- `lockmode`: The lock mode to use (not directly used in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [CreateStatistics](../C/CreateStatistics.md)
  - [AlteredTableInfo](AlteredTableInfo.md) (struct)
  - [CreateStatsStmt](../C/CreateStatsStmt.md) (struct)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - child_dependency_type

## Notes and Other Information
- Located in src/backend/commands/tablecmds.c:9242-9262
- Returns ObjectAddress of the created statistics object
- No direct SQL grammar command exists - used internally by ALTER TABLE
- Asserts that the CreateStatsStmt is already transformed
- Primarily used for AT_ReAddStatistics operations during table rebuilds
- Simple wrapper function that delegates to CreateStatistics
- Part of the extended statistics infrastructure for multivariate statistics
- Supports rebuilding statistics that become invalid after column type changes
- Function parameters tab, rel, is_rebuild, and lockmode are accepted but not used in current implementation

## Simplified Source

```c
static ObjectAddress
ATExecAddStatistics(AlteredTableInfo *tab, Relation rel,
                   CreateStatsStmt *stmt, bool is_rebuild, LOCKMODE lockmode)
{
    ObjectAddress address;

    // Validate input - stmt should be a transformed CreateStatsStmt
    Assert(IsA(stmt, CreateStatsStmt));
    Assert(stmt->transformed);

    // Delegate to standard statistics creation infrastructure
    address = CreateStatistics(stmt);

    return address;
}
```