# pltcl_argisnull

## Location
[src/pl/tcl/pltcl.c:2063-2116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L2063-L2116)

## Overview
Determines whether a specific function argument is NULL, providing NULL-checking functionality for PL/Tcl functions.

## Definition
```c
static int
pltcl_argisnull(ClientData cdata, Tcl_Interp *interp,
                int objc, Tcl_Obj *const objv[])
```

## Detailed Description
This function implements a Tcl command that allows PL/Tcl functions to check whether a specific argument passed to the function is NULL. It provides essential functionality for handling NULL values in PostgreSQL functions written in Tcl, enabling proper NULL-aware logic in stored procedures.

The function validates that it's being called in the correct context (not from a trigger), extracts and validates the argument number, and uses PostgreSQL's PG_ARGISNULL macro to check the NULL status of the specified argument. The argument number is expected to be 1-based (as typical in user interfaces) but is converted to 0-based indexing internally for the PostgreSQL function call interface.

The function performs several validation steps: ensuring correct syntax (exactly one argument number), verifying it's called from a function context (not a trigger), validating the argument number is a valid integer, and checking that the argument index is within the valid range of function arguments.

## Parameters / Member Variables
- `cdata`: ClientData passed to the Tcl command (unused)
- `interp`: Tcl interpreter context for setting results and errors
- `objc`: Number of arguments passed to the command (should be 2: command name + argument number)
- `objv`: Array of Tcl objects containing the command arguments

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCallInfo (PostgreSQL type)
  - pltcl_current_call_state (global state variable)
  - Tcl_WrongNumArgs (Tcl library function)
  - Tcl_SetObjResult (Tcl library function)
  - Tcl_NewStringObj (Tcl library function)
  - Tcl_GetIntFromObj (Tcl library function)
  - Tcl_NewBooleanObj (Tcl library function)
  - PG_ARGISNULL (PostgreSQL macro)
- Called from (representative examples):
  - TclExceptionNameMap (registered as Tcl command)
  - pltcl_init_interp (command registration)

## Notes and Other Information
- Returns TCL_ERROR if called with wrong syntax or in invalid context
- Returns TCL_OK with boolean result indicating NULL status
- Cannot be used in trigger functions (only regular functions)
- Argument numbers are 1-based in the user interface but converted to 0-based internally
- Returns "argisnull cannot be used in triggers" error when called from trigger context
- Returns "argno out of range" error for invalid argument numbers
- Registered as built-in Tcl command "argisnull" available to PL/Tcl functions
- Essential for implementing NULL-aware logic in Tcl stored procedures