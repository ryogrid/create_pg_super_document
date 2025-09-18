# ecpg_do

## Location
src/interfaces/ecpg/ecpglib/execute.c: 2243 - 2276

## Overview
The main execution function for SQL statements in the ECPG (Embedded SQL in C for PostgreSQL) library that processes variable argument lists and coordinates the complete SQL execution pipeline.

## Definition


## Detailed Description
The `ecpg_do` function serves as the core execution engine for SQL statements in the ECPG library. It orchestrates the complete process of executing embedded SQL statements by coordinating several phases: prologue setup, parameter building, transaction management, statement execution, and output processing. This function is designed to handle variable argument lists, making it suitable for use by other functions that need to pass dynamic parameters. The function follows a fail-safe pattern where any failure in the execution pipeline causes an immediate jump to cleanup code.

## Parameters / Member Variables
- `lineno`: Line number in the source code where the SQL statement appears (for error reporting)
- `compat`: Compatibility mode setting for ECPG behavior
- `force_indicator`: Flag to force indicator variable handling
- `connection_name`: Name of the database connection to use (NULL for default connection)
- `questionmarks`: Boolean flag indicating whether the query uses question mark parameter placeholders
- `st`: Statement type as an integer (cast to ECPG_statement_type enum)
- `query`: The SQL query string to execute
- `args`: Variable argument list containing parameters for the query

## Dependencies
- Functions called/Symbols referenced:
  - statement (struct type)
  - ecpg_do_prologue
  - ECPG_statement_type
  - ecpg_build_params
  - ecpg_autostart_transaction
  - ecpg_execute
  - ecpg_process_output
  - ecpg_do_epilogue
- Called from (representative examples):
  - [ECPGdo](../E/ECPGdo.md)

## Notes and Other Information
- Returns true on successful execution, false on failure
- Uses a structured execution pipeline with fail-safe error handling
- The function ensures proper cleanup through ecpg_do_epilogue regardless of success or failure
- Located in src/interfaces/ecpg/ecpglib/execute.c:2243-2276
- This is an internal function primarily used by the public ECPGdo interface