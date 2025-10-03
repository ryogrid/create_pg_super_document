# DeallocateQuery

## Location
[src/backend/commands/prepare.c:502-515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L502-L515)

## Overview
Implements the DEALLOCATE SQL utility statement by removing specified prepared statements from storage or deallocating all prepared statements.

## Definition

```c
void
DeallocateQuery(DeallocateStmt *stmt)
```
## Detailed Description
DeallocateQuery is the main entry point for executing DEALLOCATE statements in PostgreSQL. This function handles both specific prepared statement deallocation (when a name is provided) and the special case of deallocating all prepared statements (DEALLOCATE ALL). The function acts as a simple dispatcher that examines the DeallocateStmt structure and calls the appropriate underlying deallocation function based on whether a specific statement name was provided.

## Parameters / Member Variables
- `*stmt`: Pointer to a DeallocateStmt structure containing the parsed DEALLOCATE command information, including the optional statement name
## Dependencies
- Functions called/Symbols referenced:
  - [DropPreparedStatement](DropPreparedStatement.md) (for deallocating a specific named statement)
  - [DropAllPreparedStatements](DropAllPreparedStatements.md) (for deallocating all prepared statements)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (main utility command processor)

## Notes and Other Information
- Implements the high-level logic for the SQL DEALLOCATE statement
- Supports both 'DEALLOCATE statement_name' and 'DEALLOCATE ALL' variants
- Acts as a thin wrapper around the core deallocation functions
- Part of PostgreSQL's SQL utility command processing system
- The function determines behavior based on whether stmt->name is NULL or contains a statement name
- Integrated into the standard utility command processing pipeline

## Simplified Source

```c
void DeallocateQuery(DeallocateStmt *stmt) {
    if (stmt->name)
        DropPreparedStatement(stmt->name, true);  // Drop specific statement
    else
        DropAllPreparedStatements();              // Drop all statements
}
```