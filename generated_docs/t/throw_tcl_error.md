# throw_tcl_error

## Location
[src/pl/tcl/pltcl.c:1371-1399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L1371-L1399)

## Overview
Reports PostgreSQL errors from Tcl interpreter execution results, converting Tcl error information into PostgreSQL's error reporting format.

## Definition
```c
static void
throw_tcl_error(Tcl_Interp *interp, const char *proname)
```

## Detailed Description
This function serves as the error translation bridge between the Tcl interpreter and PostgreSQL's error reporting system. When a Tcl procedure execution fails, this function extracts error information from the Tcl interpreter and converts it into a PostgreSQL ERROR using the ereport mechanism.

The function carefully manages memory and string conversion to avoid potential issues with Tcl_GetVar overwriting the interpreter result. It extracts both the main error message from Tcl_GetStringResult() and the detailed error context from the Tcl "errorInfo" global variable, then formats them into a comprehensive PostgreSQL error report with proper encoding conversion from UTF-8.

## Parameters / Member Variables
- `interp`: Pointer to the Tcl interpreter that encountered the error
- `proname`: Name of the PL/Tcl function where the error occurred, used for error context

## Dependencies
- Functions called/Symbols referenced:
  - pstrdup
  - utf_u2e
  - Tcl_GetStringResult
  - Tcl_GetVar
  - ereport
  - errcode
  - errmsg
  - errcontext
- Called from (representative examples):
  - pltcl_func_handler
  - pltcl_trigger_handler
  - pltcl_event_trigger_handler

## Notes and Other Information
- Should only be used to report errors from Tcl_EvalObjEx() or similar Tcl evaluation functions
- Other Tcl functions may not properly set "errorInfo", potentially leading to stale error information
- Uses pstrdup() to create a safe copy of the error message before calling Tcl_GetVar to avoid memory corruption
- Applies UTF-8 to database encoding conversion using utf_u2e() for both error message and context
- Reports errors with ERRCODE_EXTERNAL_ROUTINE_EXCEPTION to indicate the error originated from an external routine
- Includes both the Tcl error details and the PL/Tcl function name in the error context for debugging