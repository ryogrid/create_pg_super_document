# pltcl_rollback

## Location
src/pl/tcl/pltcl.c: 2978 - 3017

## Overview
pltcl_rollback is a static function in the PL/Tcl extension that aborts the current transaction and starts a new one, providing transaction rollback capabilities to Tcl code.

## Definition
```c
static int
pltcl_rollback(ClientData cdata, Tcl_Interp *interp,
               int objc, Tcl_Obj *const objv[])
```

## Detailed Description
pltcl_rollback provides a way for PL/Tcl functions to abort the current transaction and automatically start a new one. The function wraps the PostgreSQL SPI_rollback() call with proper error handling, converting any PostgreSQL errors into Tcl exceptions. If the rollback operation fails, the function captures the error information, constructs a Tcl-compatible error code, and returns TCL_ERROR with the error message. On successful rollback, it returns TCL_OK. This function enables PL/Tcl procedures to perform explicit transaction control operations, allowing them to discard all changes made in the current transaction.

## Parameters / Member Variables
- `cdata`: ClientData passed from Tcl (unused in this function)
- `interp`: Tcl interpreter context where error messages will be set if needed
- `objc`: Number of Tcl objects in the argument array (unused)
- `objv[]`: Array of Tcl objects (unused)

## Dependencies
- Functions called/Symbols referenced:
  - SPI_rollback
  - CopyErrorData
  - FlushErrorState
  - pltcl_construct_errorCode
  - FreeErrorData
  - MemoryContextSwitchTo
  - UTF_E2U (encoding conversion)
  - Tcl_SetObjResult
  - Tcl_NewStringObj
- Called from (representative examples):
  - Registered as a Tcl command in pltcl_init_interp
  - Available to PL/Tcl functions as "rollback" command

## Notes and Other Information
- Uses PostgreSQL's PG_TRY/PG_CATCH exception handling mechanism
- Converts PostgreSQL error data into Tcl-compatible format
- Preserves memory context during error handling
- Does not validate the number or content of arguments
- Part of the PL/Tcl extension's transaction control functionality
- Should only be used in appropriate transactional contexts
- Automatically starts a new transaction after successful rollback
- Complementary function to pltcl_commit for complete transaction control