# pltcl_elog

## Location
[src/pl/tcl/pltcl.c:1759-1845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L1759-L1845)

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
  - [CopyErrorData](../C/CopyErrorData.md)
  - [FlushErrorState](../F/FlushErrorState.md)
  - [pltcl_construct_errorCode](pltcl_construct_errorCode.md)
  - [FreeErrorData](../F/FreeErrorData.md)
  - UTF_U2E
  - UTF_E2U
- Called from (representative examples):
  - [pltcl_init_interp](pltcl_init_interp.md) (registers the command)

## Notes and Other Information
- Maps Tcl log priority strings to PostgreSQL log levels using static arrays for efficient lookup
- Uses Tcl_GetIndexFromObj for robust priority string matching with exact matching required
- Special handling for ERROR level: returns TCL_ERROR instead of calling ereport to allow Tcl error handling
- Uses ERRCODE_EXTERNAL_ROUTINE_EXCEPTION for all generated PostgreSQL errors
- Implements comprehensive error recovery with proper memory context switching and error state cleanup
- Supports UTF-8 encoding conversion in both directions (Tcl to PostgreSQL and vice versa)
- FATAL errors are handled normally through ereport but will not return control to the Tcl interpreter
- Requires exactly 2 arguments (priority and message) and validates argument count with proper Tcl error reporting

## Simplified Source

```c
static int pltcl_elog(ClientData cdata, Tcl_Interp *interp,
                      int objc, Tcl_Obj *const objv[]) {
    volatile int level;
    int priIndex;

    // Map Tcl priority strings to PostgreSQL log levels
    static const char *logpriorities[] = {
        "DEBUG", "LOG", "INFO", "NOTICE",
        "WARNING", "ERROR", "FATAL", NULL
    };

    static const int loglevels[] = {
        DEBUG2, LOG, INFO, NOTICE,
        WARNING, ERROR, FATAL
    };

    // Validate argument count
    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "level msg");
        return TCL_ERROR;
    }

    // Look up priority level
    if (Tcl_GetIndexFromObj(interp, objv[1], logpriorities, "priority",
                           TCL_EXACT, &priIndex) != TCL_OK)
        return TCL_ERROR;

    level = loglevels[priIndex];

    // Handle ERROR level specially - return to Tcl for error handling
    if (level == ERROR) {
        Tcl_SetObjResult(interp, objv[2]);
        return TCL_ERROR;
    }

    // For other levels, report through PostgreSQL ereport
    MemoryContext oldcontext = CurrentMemoryContext;
    PG_TRY();
    {
        UTF_BEGIN;
        ereport(level,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("%s", UTF_U2E(Tcl_GetString(objv[2])))));
        UTF_END;
    }
    PG_CATCH();
    {
        // Handle ereport failure by constructing Tcl error
        ErrorData *edata;
        MemoryContextSwitchTo(oldcontext);
        edata = CopyErrorData();
        FlushErrorState();

        pltcl_construct_errorCode(interp, edata);
        UTF_BEGIN;
        Tcl_SetObjResult(interp, Tcl_NewStringObj(UTF_E2U(edata->message), -1));
        UTF_END;
        FreeErrorData(edata);
        return TCL_ERROR;
    }
    PG_END_TRY();

    return TCL_OK;
}
```