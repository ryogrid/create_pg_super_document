# ATRewriteCatalogs

## Location
src/backend/commands/tablecmds.c: 5158 - 5231

## Overview
ATRewriteCatalogs serves as the traffic controller for ALTER TABLE Phase 2 operations, executing subcommands in a carefully designed order to avoid conflicts and handle cross-table dependencies.

## Definition
```c
static void ATRewriteCatalogs(List **wqueue, LOCKMODE lockmode, AlterTableUtilityContext *context)
```

## Detailed Description
ATRewriteCatalogs implements the second phase of PostgreSQL's three-phase ALTER TABLE execution strategy, focusing on catalog updates and modifications. The function processes all tables in the work queue through multiple execution passes, ensuring that operations are performed in a safe order that avoids unnecessary conflicts.

The function operates on a "parallel" processing model where all tables are processed simultaneously, one pass at a time. This design is crucial for handling cross-table dependencies, such as foreign key constraints where changes to a primary key table must propagate changes to referencing tables. Work can only be propagated into later passes, maintaining execution order integrity.

Key execution phases include:
- **Multiple Pass Processing**: Executes commands in predefined passes (AT_PASS_*) to handle dependencies
- **Relation Management**: Opens and closes relations as needed, leveraging locks acquired in Phase 1
- **Cross-table Propagation**: Handles cases where changes in one table affect others
- **Post-processing Cleanup**: Performs cleanup after certain operation types
- **Toast Table Management**: Evaluates and creates toast tables when necessary

## Parameters / Member Variables
- `wqueue`: Pointer to the work queue containing all tables and their organized subcommands
- `lockmode`: The lock mode to use for operations (inherited from Phase 1)
- `context`: Utility context containing additional state and configuration information

## Dependencies
- Functions called/Symbols referenced:
  - relation_open
  - relation_close
  - ATExecCmd
  - ATPostAlterTypeCleanup
  - AlterTableCreateToastTable
  - Various AT_PASS_* constants
  - AlterTablePass enumeration
- Called from (representative examples):
  - ATController

## Notes and Other Information
- Static function scope indicating internal implementation detail
- Uses NoLock when opening relations since appropriate locks were acquired in Phase 1
- Special cleanup handling for ALTER TYPE and SET EXPRESSION operations
- Toast table creation logic considers relation kind and partition constraints
- The parallel processing model enables complex dependency resolution
- Relations are opened and closed for each table during processing to allow subroutines flexibility
- Work propagation between tables can only occur into later passes, ensuring dependency order
- Excludes tables that are sources of ATTACH PARTITION commands from toast table evaluation
- Supports materialized views and regular tables for toast table creation