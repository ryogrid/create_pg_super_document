# ExecuteStmt

## Location
[src/include/nodes/parsenodes.h:4044-4049](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4044-L4049)

## Overview
ExecuteStmt represents the parsed form of an EXECUTE SQL statement, which executes a previously prepared statement with specific parameter values.

## Definition
```c
typedef struct ExecuteStmt
{
    NodeTag     type;
    char       *name;        /* The name of the plan to execute */
    List       *params;      /* Values to assign to parameters */
} ExecuteStmt;
```

## Detailed Description
ExecuteStmt is a parse node structure that holds the information needed to execute a prepared statement in PostgreSQL. When a PREPARE statement creates a prepared statement, subsequent EXECUTE statements reference that prepared statement by name and provide actual parameter values to be substituted for the parameter placeholders.

This structure is created during the parsing phase of an EXECUTE statement and is later processed by the execute command system to lookup the prepared statement and execute it with the provided parameters.

## Parameters / Member Variables
- `type`: Standard NodeTag for parse tree node identification
- `name`: String containing the name of the prepared statement to execute
- `params`: List of parameter values (expressions) to substitute for parameter placeholders

## Dependencies
- Functions called/Symbols referenced:
  - List (PostgreSQL list structure)
- Called from (representative examples):
  - ExecuteQuery (command execution function)  
  - ExplainExecuteQuery (for EXPLAIN EXECUTE statements)
  - standard_ProcessUtility (utility command dispatcher)
  - ExecCreateTableAs (for CREATE TABLE AS EXECUTE)
  - FetchStatementTargetList (for retrieving result column info)

## Notes and Other Information
- Part of the SQL prepared statement functionality in PostgreSQL
- Located in src/include/nodes/parsenodes.h along with other statement structures
- The actual execution logic is implemented in ExecuteQuery() in src/backend/commands/prepare.c
- Parameter values are evaluated in the current execution context before being passed to the prepared statement
- Works in conjunction with PrepareStmt and DeallocateStmt to provide complete prepared statement functionality
- Can be used with EXPLAIN to analyze execution plans of prepared statements