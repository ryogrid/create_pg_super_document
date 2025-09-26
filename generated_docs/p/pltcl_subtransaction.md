# pltcl_subtransaction

## Location
[src/pl/tcl/pltcl.c:2891-2938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L2891-L2938)

## Overview
pltcl_subtransaction is a static function in the PL/Tcl extension that executes Tcl code within a subtransaction, providing transactional isolation for the executed code.

## Definition
```c
static int
pltcl_subtransaction(ClientData cdata, Tcl_Interp *interp,
                     int objc, Tcl_Obj *const objv[])
```

## Detailed Description
pltcl_subtransaction executes a Tcl code fragment within the safety of a PostgreSQL subtransaction. The function creates a subtransaction, executes the provided Tcl command, and then either commits or aborts the subtransaction based on the execution result. If the Tcl code returns TCL_ERROR, the subtransaction is rolled back; otherwise, it is committed. This provides a way for PL/Tcl functions to safely execute code that might fail without affecting the outer transaction. Unlike the pltcl_subtrans_* helper functions, this function implements its own subtransaction handling to avoid the error handling mechanisms in pltcl_subtrans_abort.

## Parameters / Member Variables
- `cdata`: ClientData passed from Tcl (unused in this function)
- `interp`: Tcl interpreter context where the command will be executed
- `objc`: Number of Tcl objects in the argument array
- `objv[]`: Array of Tcl objects containing the command to execute

## Dependencies
- Functions called/Symbols referenced:
  - BeginInternalSubTransaction
  - RollbackAndReleaseCurrentSubTransaction
  - ReleaseCurrentSubTransaction
  - MemoryContextSwitchTo
  - Tcl_EvalObjEx
  - Tcl_WrongNumArgs
- Called from (representative examples):
  - Registered as a Tcl command in pltcl_init_interp
  - Available to PL/Tcl functions as "subtransaction" command

## Notes and Other Information
- Expects exactly 2 arguments: the command name and the Tcl code to execute
- Does not use the standard pltcl_subtrans_* helper functions to avoid their error handling
- Preserves and restores memory context and resource owner state
- Returns the same return code as the executed Tcl command
- Provides transactional safety for potentially dangerous Tcl operations
- Part of the PL/Tcl extension's transaction management capabilities