# interpret_func_parallel

## Location
src/backend/commands/functioncmds.c: 620 - 644

## Overview
Converts string-based parallel safety specifications from CREATE FUNCTION or ALTER FUNCTION statements into the corresponding internal character constants used by PostgreSQL's catalog system for parallel query execution.

## Definition


## Detailed Description
This function processes parallel safety attributes specified in function definitions and translates user-facing keywords ("safe", "unsafe", "restricted") into internal PostgreSQL constants. The parallel safety designation determines how the function can be used in parallel query execution. The function enforces strict validation by reporting a syntax error with a descriptive message if an invalid parallel specification is provided, ensuring only the three valid options are accepted.

## Parameters / Member Variables
- : DefElem containing the parallel safety specification with a string argument

## Dependencies
- Functions called/Symbols referenced:
  - strVal: Extracts string value from the DefElem argument
  - strcmp: Compares input string with known parallel safety values
  - ereport: Reports structured errors for invalid parallel specifications
  - errcode: Provides ERRCODE_SYNTAX_ERROR for invalid specifications
  - errmsg: Formats error message with valid options
  - PROPARALLEL_SAFE: Constant for parallel-safe functions
  - PROPARALLEL_UNSAFE: Constant for functions unsafe for parallel execution
  - PROPARALLEL_RESTRICTED: Constant for functions with restricted parallel usage
- Called from (representative examples):
  - compute_function_attributes: During function creation attribute processing
  - AlterFunction: During function alteration attribute processing

## Notes and Other Information
- Returns character constants used in the pg_proc system catalog's 'proparallel' column
- PARALLEL SAFE functions can be executed in parallel workers without restrictions
- PARALLEL UNSAFE functions cannot be executed in parallel workers at all
- PARALLEL RESTRICTED functions can be executed in parallel workers but with limitations (e.g., cannot write to database)
- Uses ereport instead of elog to provide structured error reporting with proper error codes
- The error message explicitly lists all valid options to guide users
- Invalid specifications terminate the current transaction with a syntax error
- Default return value of PROPARALLEL_UNSAFE ensures conservative behavior if function somehow continues after error