# AlterTable

## Location
src/backend/commands/tablecmds.c: 4399 - 4427

## Overview
The main entry point for executing ALTER TABLE commands, coordinating a sophisticated three-phase process to safely modify table structures while minimizing data access overhead.

## Definition
```c
void AlterTable(AlterTableStmt *stmt, LOCKMODE lockmode, AlterTableUtilityContext *context)
```

## Detailed Description
AlterTable serves as the primary coordinator for all ALTER TABLE operations in PostgreSQL. It implements a carefully designed three-phase execution model that allows multiple independent schema changes to be performed efficiently with only a single pass over the table data when necessary.

**Three-Phase Architecture:**

1. **Phase 1 (ATPrepCmd)**: Examination and pre-transformation checking
   - Acquires table locks and checks permissions
   - Performs preliminary validation of subcommands
   - Creates work queue entries for affected tables (including inheritance hierarchy)
   - Divides subcommands into logical "passes" to avoid conflicts
   - Recurses to find child tables in inheritance hierarchies

2. **Phase 2 (ATRewriteCatalogs)**: Validation, transformation, and catalog updates
   - Processes subcommands in the correct order to avoid unnecessary conflicts
   - Updates system catalogs with schema changes
   - Handles subcommand ordering (e.g., DROP COLUMN before ADD COLUMN)

3. **Phase 3 (ATRewriteTables)**: Data scanning and optional table rewriting
   - Only performed when subcommands require data validation or reorganization
   - Scans tables to check new constraints
   - Optionally rewrites table data into new storage

The design philosophy centers on allowing multiple independent updates to be batched together, minimizing expensive data access operations. The function leverages PostgreSQL's MVCC system for automatic rollback on errors, eliminating the need for explicit cleanup code.

## Parameters / Member Variables
- `stmt`: The parsed ALTER TABLE statement containing the list of subcommands to execute
- `lockmode`: The lock level required for the operation (determined by AlterTableGetLockLevel)
- `context`: Execution context containing relation OID and other state needed for subcommand execution

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md) (opens the target relation using the provided OID)
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md) (performs safety checks before allowing ALTER operations)
  - [ATController](ATController.md) (orchestrates the three-phase execution process)
  - AlterTableStmt (structure type for parsed ALTER TABLE statements)
  - AlterTableUtilityContext (context structure for execution state)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (in utility.c:1318, during utility command processing)

## Notes and Other Information
- The caller is responsible for acquiring an appropriate lock level before calling this function
- Lock levels are passed down recursively to inherited tables rather than being reassessed at each level
- The function assumes the caller has already looked up the relation OID and provided it in the context
- Uses NoLock when opening the relation since the caller has already acquired the necessary lock
- The three-phase design enables complex operations like changing column types while minimizing performance impact
- Inheritance hierarchy traversal is handled automatically, ensuring changes propagate correctly to child tables
- The work queue mechanism allows for proper ordering of operations and efficient batch processing
- Error handling relies on PostgreSQL's transaction system for automatic cleanup and rollback
- Some subcommands set recurse flags during phase 1 when recursion logic can only be determined during phase 2
- The architecture supports complex scenarios like adding constraints that require data validation across multiple tables