# output_deallocate_prepare_statement

## Location
[src/interfaces/ecpg/preproc/output.c:182-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/output.c#L182-L199)

## Overview
The output_deallocate_prepare_statement function generates C code that calls ECPGdeallocate or ECPGdeallocate_all runtime functions to deallocate prepared SQL statements in embedded SQL programs.

## Definition
void output_deallocate_prepare_statement(char *name)

## Detailed Description
This function is part of the ECPG (Embedded SQL in C) preprocessor and generates C code to deallocate prepared statements at runtime. It handles two cases: deallocating a specific named prepared statement using ECPGdeallocate, or deallocating all prepared statements when the name is "all" using ECPGdeallocate_all. The function properly escapes the statement name for inclusion in generated C code and includes connection and compatibility information for the runtime system.

## Parameters / Member Variables
- `name`: The name of the prepared statement to deallocate, or "all" to deallocate all prepared statements

## Dependencies
- Functions called/Symbols referenced:
  - [output_escaped_str](output_escaped_str.md) (for escaping the statement name when not "all")
  - [whenever_action](../w/whenever_action.md) (for generating error handling code)
- Called from (representative examples):
  - No direct callers found in the indexed symbols

## Notes and Other Information
- Special handling for the "all" keyword: when name equals "all", calls ECPGdeallocate_all instead of ECPGdeallocate
- The function uses the global `connection` variable, outputting "NULL" if no connection is set
- The global `compat` flag is included in both ECPGdeallocate and ECPGdeallocate_all calls
- Statement name escaping is only performed for non-"all" cases using output_escaped_str with quoted=true
- Memory management: The function calls free() on the name parameter after processing
- The generated code includes line number information for debugging purposes
- Error handling is managed through whenever_action(2) call