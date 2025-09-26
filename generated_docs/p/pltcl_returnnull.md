# pltcl_returnnull

## Location
src/pl/tcl/pltcl.c: 2117 - 2154

## Overview
Causes the current PL/Tcl function to return a NULL value by setting the NULL flag and returning from the procedure.

## Definition
```c
static int
pltcl_returnnull(ClientData cdata, Tcl_Interp *interp,
                 int objc, Tcl_Obj *const objv[])
```

## Detailed Description
This function implements a Tcl command that provides a clean mechanism for PL/Tcl functions to return NULL values. When called, it sets the PostgreSQL function call's NULL flag (fcinfo->isnull) to true and returns TCL_RETURN, which causes the Tcl interpreter to immediately return from the current procedure with the NULL status properly propagated to PostgreSQL.

The function serves as an alternative to simply returning an empty string or other value when the intent is to return a database NULL. This is particularly important in PostgreSQL where NULL has specific semantic meaning different from empty strings or zero values. The command provides a clear, explicit way for Tcl functions to signal NULL returns.

The function validates that it's being called in the correct context (not from a trigger function) and ensures no arguments are passed to the command. Upon successful validation, it sets the NULL flag in the function call information structure and returns with TCL_RETURN to exit the procedure immediately.

## Parameters / Member Variables
- `cdata`: ClientData passed to the Tcl command (unused)
- `interp`: Tcl interpreter context for setting error messages
- `objc`: Number of arguments passed to the command (should be 1: just command name)
- `objv`: Array of Tcl objects containing the command arguments

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCallInfo (PostgreSQL type)
  - pltcl_current_call_state (global state variable)
  - Tcl_WrongNumArgs (Tcl library function)
  - Tcl_SetObjResult (Tcl library function)
  - Tcl_NewStringObj (Tcl library function)
- Called from (representative examples):
  - TclExceptionNameMap (registered as Tcl command)
  - pltcl_init_interp (command registration)

## Notes and Other Information
- Returns TCL_ERROR if called with arguments or in invalid context
- Returns TCL_RETURN on successful NULL return setup, causing immediate procedure exit
- Cannot be used in trigger functions (only regular functions)
- Sets fcinfo->isnull = true to indicate NULL return to PostgreSQL
- Returns "return_null cannot be used in triggers" error when called from trigger context
- Registered as built-in Tcl command "return_null" available to PL/Tcl functions
- Provides explicit NULL return semantics distinct from returning empty values
- Essential for proper NULL handling in Tcl stored procedures where NULL has specific database meaning