# pltcl_returnnext

## Location
src/pl/tcl/pltcl.c: 2155 - 2277

## Overview
Adds a row to the result tuplestore in a set-returning function, enabling PL/Tcl functions to return multiple rows.

## Definition
```c
static int
pltcl_returnnext(ClientData cdata, Tcl_Interp *interp,
                 int objc, Tcl_Obj *const objv[])
```

## Detailed Description
This function implements a Tcl command that enables PL/Tcl set-returning functions (SRFs) to incrementally return multiple rows. It validates that the function is called in the appropriate context (set-returning function, not a trigger), processes the provided result value, and adds it to the function's tuple store for later retrieval by PostgreSQL.

The function handles both scalar and tuple return types. For scalar returns, it processes the single value through the appropriate input function and stores it. For tuple returns, it expects the input to be a Tcl list representing the row values, converts this to a HeapTuple, and stores it in the tuplestore.

The operation is performed within a subtransaction to ensure proper error handling and resource management. This allows the function to safely handle potential errors during tuple construction or storage without affecting the main transaction. The subtransaction also provides a short-lived memory context that automatically cleans up temporary allocations.

The function initializes the tuple store on the first call, manages memory contexts and resource owners appropriately, and uses PostgreSQL's exception handling mechanisms to ensure proper cleanup in case of errors.

## Parameters / Member Variables
- `cdata`: ClientData passed to the Tcl command (unused)
- `interp`: Tcl interpreter context for setting results and errors
- `objc`: Number of arguments passed to the command (should be 2: command name + result)
- `objv`: Array of Tcl objects containing the command arguments

## Dependencies
- Functions called/Symbols referenced:
  - pltcl_call_state (PL/Tcl call state structure)
  - FunctionCallInfo (PostgreSQL function call info)
  - pltcl_proc_desc (PL/Tcl procedure descriptor)
  - ResourceOwner (PostgreSQL resource management)
  - BeginInternalSubTransaction (PostgreSQL subtransaction management)
  - PG_TRY/PG_CATCH/PG_END_TRY (PostgreSQL exception handling macros)
  - pltcl_init_tuple_store (initializes tuple storage)
  - Tcl_Size (Tcl type definition)
  - pltcl_build_tuple_result (constructs tuple from Tcl values)
  - tuplestore_puttuple (stores tuple in tuplestore)
  - InputFunctionCall (converts text to PostgreSQL datum)
  - utf_u2e (UTF-8 encoding conversion)
  - tuplestore_putvalues (stores values in tuplestore)
  - pltcl_subtrans_commit (commits subtransaction)
  - pltcl_subtrans_abort (aborts subtransaction)
- Called from (representative examples):
  - TclExceptionNameMap (registered as Tcl command)
  - pltcl_init_interp (command registration)

## Notes and Other Information
- Returns TCL_ERROR for invalid usage or processing errors
- Returns TCL_OK on successful row addition
- Can only be used in set-returning functions (not triggers or regular functions)
- Initializes tuple store automatically on first call
- Handles both scalar and tuple return types differently
- Uses subtransactions for safe error handling and memory management
- For tuple returns, expects input as Tcl list with appropriate number of elements
- For scalar returns, processes single value through input function
- Registered as built-in Tcl command "return_next" available to PL/Tcl SRFs
- Essential for implementing set-returning functions in PL/Tcl
- Provides incremental row return capability for streaming large result sets
- Error messages include specific context about usage restrictions