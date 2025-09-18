# DropDatabase

## Location
src/backend/commands/dbcommands.c: 2303 - 2327

## Overview
DropDatabase is a wrapper function that processes DROP DATABASE statement options and delegates the actual database deletion to the dropdb function.

## Definition
```c
void DropDatabase(ParseState *pstate, DropdbStmt *stmt)
```

## Detailed Description
DropDatabase serves as the entry point for DROP DATABASE statement execution, parsing the statement's options (currently only supporting the 'force' option) and calling the lower-level dropdb function to perform the actual database deletion. It validates statement options and provides proper error reporting with positional information for unrecognized options. This separation allows for clean abstraction between SQL statement parsing and the core database deletion logic.

## Parameters / Member Variables
- `pstate`: Parser state for error reporting and context information
- `stmt`: DropdbStmt structure containing the database name, missing_ok flag, and option list

## Dependencies
- Functions called/Symbols referenced:
  - DropdbStmt: Statement structure containing database drop parameters
  - DefElem: Definition element structure for parsing options
  - dropdb: Core function that performs the actual database deletion operation
- Called from (representative examples):
  - standard_ProcessUtility: Main utility statement processing function

## Notes and Other Information
- Currently recognizes only the 'force' option which allows terminating existing connections
- Provides parser-level error reporting with location information for syntax errors
- Acts as a thin wrapper around the dropdb function which contains the core deletion logic
- The 'force' option enables automatic termination of other database connections before dropping
- Part of PostgreSQL's utility command processing infrastructure