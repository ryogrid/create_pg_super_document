# AlterTableUtilityContext

## Location
[src/include/tcop/utility.h:30-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tcop/utility.h#L30-L37)

## Overview
AlterTableUtilityContext is a structure that carries essential context information when PostgreSQL recursively processes ALTER TABLE commands, providing access to the original command's execution environment and parameters.

## Definition
```c
typedef struct AlterTableUtilityContext
{
    PlannedStmt *pstmt;             /* PlannedStmt for outer ALTER TABLE command */
    const char *queryString;        /* its query string */
    Oid         relid;              /* OID of ALTER's target table */
    ParamListInfo params;           /* any parameters available to ALTER TABLE */
    QueryEnvironment *queryEnv;    /* execution environment for ALTER TABLE */
} AlterTableUtilityContext;
```

## Detailed Description
AlterTableUtilityContext serves as a container for preserving execution context during ALTER TABLE operations that require recursive processing. When PostgreSQL processes complex ALTER TABLE statements that may involve multiple sub-operations or inheritance hierarchies, this structure ensures that nested utility command executions have access to the original command's context, parameters, and execution environment.

The structure is particularly important for maintaining consistency and proper parameter binding when ALTER TABLE operations cascade to child tables or trigger additional utility commands. It acts as a bridge between the top-level ALTER TABLE command and any subsidiary operations that need to be executed with the same context.

## Parameters / Member Variables
- `pstmt`: Pointer to the PlannedStmt structure representing the outer ALTER TABLE command, containing the planned execution tree and metadata
- `queryString`: The original SQL query string of the ALTER TABLE command, used for logging and error reporting
- `relid`: Object identifier (OID) of the target table being altered, providing a direct reference to the table in the system catalogs
- `params`: Parameter list information containing any bound parameters that were provided with the original ALTER TABLE statement
- `queryEnv`: Execution environment containing named relations and other environmental context needed for proper query execution

## Dependencies
- Functions called/Symbols referenced:
  - [PlannedStmt](../P/PlannedStmt.md) - The planned statement structure containing execution plan details
  - [ParamListInfo](../P/ParamListInfo.md) - Parameter information framework for query parameters
  - [QueryEnvironment](../Q/QueryEnvironment.md) - [Query](../Q/Query.md) execution environment container
- Called from (representative examples):
  - [AlterTable](AlterTable.md) - Main ALTER TABLE processing function
  - [ATController](ATController.md) - ALTER TABLE controller managing the overall process
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) - Slow path utility statement processing
  - [ProcessUtilityForAlterTable](../P/ProcessUtilityForAlterTable.md) - Specialized ALTER TABLE utility processing
  - Various AT* functions - Specific ALTER TABLE operation handlers

## Notes and Other Information
- This structure is defined in src/include/tcop/utility.h at lines 29-37
- Essential for maintaining execution context during complex ALTER TABLE operations
- Used extensively throughout the table command processing infrastructure in src/backend/commands/tablecmds.c
- Provides a clean interface for passing context between different levels of ALTER TABLE processing
- The structure helps ensure that recursive operations maintain proper transaction semantics and parameter binding
- Critical for inheritance-based operations where ALTER TABLE commands need to cascade to child tables