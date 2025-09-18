# output_prepare_statement

## Location
[src/interfaces/ecpg/preproc/output.c:170-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/output.c#L170-L181)

## Overview
The output_prepare_statement function generates C code that calls the ECPGprepare runtime function to prepare SQL statements for later execution in embedded SQL programs.

## Definition
void output_prepare_statement(char *name, char *stmt)

## Detailed Description
This function is part of the ECPG (Embedded SQL in C) preprocessor and generates C code to prepare SQL statements at runtime. It outputs calls to the ECPGprepare function, which parses and prepares SQL statements for efficient repeated execution. The function properly escapes both the prepared statement name and the SQL statement text for inclusion in generated C code, and includes connection and question mark count information for the runtime system.

## Parameters / Member Variables
- `name`: The name to assign to the prepared statement (used for later reference)
- `stmt`: The SQL statement string to be prepared

## Dependencies
- Functions called/Symbols referenced:
  - [output_escaped_str](output_escaped_str.md) (called twice: once for escaping the statement name, once for escaping the SQL statement)
  - [whenever_action](../w/whenever_action.md) (for generating error handling code)
- Called from (representative examples):
  - No direct callers found in the indexed symbols

## Notes and Other Information
- The function uses the global `connection` variable, outputting "NULL" if no connection is set
- The global `questionmarks` count is included in the generated ECPGprepare call
- Both the statement name and SQL text are escaped using output_escaped_str with the quoted=true parameter
- Memory management: The function calls free() on the name parameter after processing
- The generated code includes line number information for debugging purposes
- Error handling is managed through whenever_action(2) call