# PLy_procedure_name

## Location
src/pl/plpython/plpy_procedure.c: 49 - 68

## Overview
Returns the SQL name of a specified PL/Python procedure, providing a safe way to retrieve procedure names for error reporting and debugging purposes.

## Definition
```c
char *PLy_procedure_name(PLyProcedure *proc)
```

## Detailed Description
This function extracts and returns the SQL name (not the internal Python procedure name) from a PLyProcedure structure. It includes null-pointer safety by returning a descriptive string when passed a NULL procedure pointer. The function is primarily used in error reporting contexts where the procedure name needs to be displayed to users or logged for debugging purposes.

## Parameters / Member Variables
- `proc`: Pointer to a PLyProcedure structure containing the procedure information. Can be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - [PLyProcedure](PLyProcedure.md) (procedure structure type)
- Called from (representative examples):
  - [PLy_traceback](PLy_traceback.md) (error traceback generation)
  - [plpython_error_callback](../p/plpython_error_callback.md) (error callback handler)

## Notes and Other Information
- Returns "<unknown procedure>" when passed a NULL pointer for safety
- Returns the SQL procedure name (proc->proname), not the internal Python function name
- Commonly used in error handling and debugging contexts
- The returned string is owned by the PLyProcedure structure and should not be freed by the caller
- Essential for providing meaningful error messages to users when PL/Python procedures encounter issues