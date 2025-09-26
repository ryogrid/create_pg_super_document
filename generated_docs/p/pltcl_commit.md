# pltcl_commit

## Location
[src/pl/tcl/pltcl.c:2939-2977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L2939-L2977)

## Overview
pltcl_commit is a static function in the PL/Tcl extension that commits the current transaction and starts a new one, providing transaction control capabilities to Tcl code.

## Definition
```c
static int
pltcl_commit(ClientData cdata, Tcl_Interp *interp,
             int objc, Tcl_Obj *const objv[])
```

## Detailed Description
pltcl_commit provides a way for PL/Tcl functions to commit the current transaction and automatically start a new one. The function wraps the PostgreSQL SPI_commit() call with proper error handling, converting any PostgreSQL errors into Tcl exceptions. If the commit operation fails, the function captures the error information, constructs a Tcl-compatible error code, and returns TCL_ERROR with the error message. On successful commit, it returns TCL_OK. This function enables PL/Tcl procedures to perform explicit transaction control operations.

## Parameters / Member Variables
- `cdata`: ClientData passed from Tcl (unused in this function)
- `interp`: Tcl interpreter context where error messages will be set if needed
- `objc`: Number of Tcl objects in the argument array (unused)
- `objv[]`: Array of Tcl objects (unused)

## Dependencies
- Functions called/Symbols referenced:
  - [SPI_commit](../S/SPI_commit.md)
  - [CopyErrorData](../C/CopyErrorData.md)
  - [FlushErrorState](../F/FlushErrorState.md)
  - [pltcl_construct_errorCode](pltcl_construct_errorCode.md)
  - [FreeErrorData](../F/FreeErrorData.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - UTF_E2U (encoding conversion)
  - Tcl_SetObjResult
  - Tcl_NewStringObj
- Called from (representative examples):
  - Registered as a Tcl command in pltcl_init_interp
  - Available to PL/Tcl functions as "commit" command

## Notes and Other Information
- Uses PostgreSQL's PG_TRY/PG_CATCH exception handling mechanism
- Converts PostgreSQL error data into Tcl-compatible format
- Preserves memory context during error handling
- Does not validate the number or content of arguments
- Part of the PL/Tcl extension's transaction control functionality
- Should only be used in appropriate transactional contexts
- Automatically starts a new transaction after successful commit