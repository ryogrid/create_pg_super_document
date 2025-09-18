# output_statement

## Location
[src/interfaces/ecpg/preproc/output.c:136-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/output.c#L136-L169)

## Overview
The output_statement function generates C code that calls the ECPGdo runtime function to execute SQL statements within embedded SQL programs.

## Definition
void output_statement(char *stmt, int whenever_mode, enum ECPG_statement_type st)

## Detailed Description
This function is part of the ECPG (Embedded SQL in C) preprocessor and is responsible for generating C code that will execute SQL statements at runtime. It outputs calls to the ECPGdo function with appropriate parameters including line numbers, compatibility mode, connection information, and statement type. The function handles different statement types (normal, prepared, execute, execute immediate) and properly escapes SQL statement strings for C code generation.

## Parameters / Member Variables
- `stmt`: The SQL statement string to be executed
- `whenever_mode`: Mode flag controlling error handling behavior 
- `st`: Enumerated type indicating the kind of SQL statement (normal, prepared, execute, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [output_escaped_str](output_escaped_str.md) (for escaping SQL statement strings)
  - [dump_variables](../d/dump_variables.md) (for outputting input and result variable information)
  - [reset_variables](../r/reset_variables.md) (for cleaning up variable state)
  - [whenever_action](../w/whenever_action.md) (for generating error handling code)
  - ECPGst_prepnormal, ECPGst_execute, ECPGst_exec_immediate (statement type constants)
- Called from (representative examples):
  - No direct callers found in the indexed symbols

## Notes and Other Information
- The function handles auto_prepare mode by converting ECPGst_prepnormal to ECPGst_normal when auto_prepare is disabled
- For EXECUTE and EXECUTE IMMEDIATE statements, the statement string is output directly without escaping
- The function outputs both input variables (argsinsert) and result variables (argsresult) to the generated C code
- Memory management: The function calls free() on the stmt parameter after processing
- The generated code includes line number information for debugging purposes