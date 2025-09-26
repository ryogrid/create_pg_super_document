# pltcl_elog

## Location
src/pl/tcl/pltcl.c: 1759 - 1845

## Overview
Provides PostgreSQL elog functionality to PL/Tcl procedures, allowing Tcl code to generate PostgreSQL log messages and errors with proper severity levels.

## Definition
```c
static int
pltcl_elog(ClientData cdata, Tcl_Interp *interp,
          int objc, Tcl_Obj *const objv[])
```

## Detailed Description
This function implements the "elog" Tcl command that allows PL/Tcl procedures to generate PostgreSQL log messages and errors. It serves as a bridge between Tcl's command interface and PostgreSQL's ereport/elog system, translating Tcl string arguments into appropriate PostgreSQL log levels and messages.

The function accepts two arguments: a priority level (DEBUG, LOG, INFO, NOTICE, WARNING, ERROR, or FATAL) and a message string. For ERROR level messages, it returns TCL_ERROR to propagate the error through the Tcl interpreter rather than calling ereport directly. For all other levels, it calls ereport with proper UTF-8 encoding conversion and comprehensive error handling using PG_TRY/PG_CATCH blocks.

When ereport itself fails (which is rare), the function catches the PostgreSQL error, constructs appropriate Tcl error information using pltcl_construct_errorCode, and returns the error message to the Tcl interpreter with proper encoding conversion.

## Parameters / Member Variables
- `cdata`: Client data passed to the Tcl command (unused in this implementation)
- `interp`: Tcl interpreter where the command is being executed
- `objc`: Number of arguments passed to the Tcl command (must be exactly 3)
- `objv[]`: Array of Tcl objects containing the command arguments (objv[1] = priority, objv[2] = message)

## Dependencies
- Functions called/Symbols referenced:
  - Tcl_WrongNumArgs
  - Tcl_GetIndexFromObj
  - Tcl_SetObjResult
  - ereport
  - CopyErrorData
  - FlushErrorState
  - pltcl_construct_errorCode
  - FreeErrorData
  - UTF_U2E
  - UTF_E2U
- Called from (representative examples):
  - pltcl_init_interp (registers the command)

## Notes and Other Information
- Maps Tcl log priority strings to PostgreSQL log levels using static arrays for efficient lookup
- Uses Tcl_GetIndexFromObj for robust priority string matching with exact matching required
- Special handling for ERROR level: returns TCL_ERROR instead of calling ereport to allow Tcl error handling
- Uses ERRCODE_EXTERNAL_ROUTINE_EXCEPTION for all generated PostgreSQL errors
- Implements comprehensive error recovery with proper memory context switching and error state cleanup
- Supports UTF-8 encoding conversion in both directions (Tcl to PostgreSQL and vice versa)
- FATAL errors are handled normally through ereport but will not return control to the Tcl interpreter
- Requires exactly 2 arguments (priority and message) and validates argument count with proper Tcl error reporting