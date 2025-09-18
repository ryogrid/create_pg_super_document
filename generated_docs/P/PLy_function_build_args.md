# PLy_function_build_args

## Location
[src/pl/plpython/plpy_exec.c:435-497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L435-L497)

## Overview
PLy_function_build_args constructs a Python list of arguments from PostgreSQL function call parameters, handling type conversion and null values for PL/Python function execution.

## Definition


## Detailed Description
This function is a helper that prepares function arguments for PL/Python function execution. It performs several critical tasks:

1. **Argument List Creation**: Creates a Python list to hold all function arguments, sized according to the procedure's argument count
2. **Type Conversion**: Converts each PostgreSQL Datum argument to its corresponding Python object using the appropriate input conversion function
3. **Null Handling**: Properly handles NULL arguments by converting them to Python None objects
4. **Named Argument Support**: If the function has named arguments, sets them in the procedure's global namespace dictionary for access by name within the Python function
5. **Error Safety**: Uses PostgreSQL's exception handling to ensure proper cleanup of Python objects if conversion fails

The function ensures that all arguments are properly converted and available to the Python function both as a positional argument list and (optionally) as named variables in the global namespace.

## Parameters / Member Variables
- : FunctionCallInfo structure containing the actual argument values and null flags
- : PLyProcedure structure containing argument metadata, conversion functions, and global namespace

## Dependencies
- Functions called/Symbols referenced:
  - PyList_New, PyList_SetItem (Python list operations)
  - [PLy_input_convert](PLy_input_convert.md) (type conversion from PostgreSQL to Python)
  - PyDict_SetItemString (for setting named arguments in globals)
  - PLy_elog (error reporting)
  - PG_TRY/PG_CATCH/PG_END_TRY (exception handling)
- Called from (representative examples):
  - [PLy_exec_function](PLy_exec_function.md) (main function execution)

## Notes and Other Information
- Returns a Python list object containing the converted arguments
- Handles both positional and named argument access patterns
- Uses early allocation of the Python list before the PG_TRY block to enable quick NULL return on memory allocation failure
- Ensures proper cleanup of partially constructed argument lists on error
- Named arguments (if present) are stored in the procedure's global dictionary for Python function access
- File location: src/pl/plpython/plpy_exec.c:435-497