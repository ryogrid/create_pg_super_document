# pltcl_returnnext

## Location
[src/pl/tcl/pltcl.c:2155-2277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L2155-L2277)

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
  - [pltcl_call_state](pltcl_call_state.md) (PL/Tcl call state structure)
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (PostgreSQL function call info)
  - [pltcl_proc_desc](pltcl_proc_desc.md) (PL/Tcl procedure descriptor)
  - [ResourceOwner](../R/ResourceOwner.md) (PostgreSQL resource management)
  - [BeginInternalSubTransaction](../B/BeginInternalSubTransaction.md) (PostgreSQL subtransaction management)
  - PG_TRY/PG_CATCH/PG_END_TRY (PostgreSQL exception handling macros)
  - [pltcl_init_tuple_store](pltcl_init_tuple_store.md) (initializes tuple storage)
  - Tcl_Size (Tcl type definition)
  - [pltcl_build_tuple_result](pltcl_build_tuple_result.md) (constructs tuple from Tcl values)
  - [tuplestore_puttuple](../t/tuplestore_puttuple.md) (stores tuple in tuplestore)
  - [InputFunctionCall](../I/InputFunctionCall.md) (converts text to PostgreSQL datum)
  - [utf_u2e](../u/utf_u2e.md) (UTF-8 encoding conversion)
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md) (stores values in tuplestore)
  - [pltcl_subtrans_commit](pltcl_subtrans_commit.md) (commits subtransaction)
  - [pltcl_subtrans_abort](pltcl_subtrans_abort.md) (aborts subtransaction)
- Called from (representative examples):
  - TclExceptionNameMap (registered as Tcl command)
  - [pltcl_init_interp](pltcl_init_interp.md) (command registration)

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

## Simplified Source

```c
static int pltcl_returnnext(ClientData cdata, Tcl_Interp *interp,
                           int objc, Tcl_Obj *const objv[]) {
    pltcl_call_state *call_state = pltcl_current_call_state;
    FunctionCallInfo fcinfo = call_state->fcinfo;
    pltcl_proc_desc *prodesc = call_state->prodesc;
    MemoryContext oldcontext = CurrentMemoryContext;
    ResourceOwner oldowner = CurrentResourceOwner;
    volatile int result = TCL_OK;

    // Validate context: must be in set-returning function
    if (fcinfo == NULL) {
        Tcl_SetObjResult(interp,
                        Tcl_NewStringObj("return_next cannot be used in triggers", -1));
        return TCL_ERROR;
    }

    if (!prodesc->fn_retisset) {
        Tcl_SetObjResult(interp,
                        Tcl_NewStringObj("return_next cannot be used in non-set-returning functions", -1));
        return TCL_ERROR;
    }

    // Validate argument count
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "result");
        return TCL_ERROR;
    }

    // Use subtransaction for safe error handling
    BeginInternalSubTransaction(NULL);
    PG_TRY();
    {
        // Initialize tuple store on first call
        if (call_state->tuple_store == NULL)
            pltcl_init_tuple_store(call_state);

        if (prodesc->fn_retistuple) {
            // Handle tuple return: convert Tcl list to HeapTuple
            Tcl_Obj **rowObjv;
            Tcl_Size rowObjc;

            if (Tcl_ListObjGetElements(interp, objv[1], &rowObjc, &rowObjv) == TCL_ERROR)
                result = TCL_ERROR;
            else {
                HeapTuple tuple = pltcl_build_tuple_result(interp, rowObjv, rowObjc, call_state);
                tuplestore_puttuple(call_state->tuple_store, tuple);
            }
        } else {
            // Handle scalar return: convert single value
            if (call_state->ret_tupdesc->natts != 1)
                elog(ERROR, "wrong result type supplied in return_next");

            Datum retval = InputFunctionCall(&prodesc->result_in_func,
                                           utf_u2e((char *) Tcl_GetString(objv[1])),
                                           prodesc->result_typioparam, -1);
            bool isNull = false;
            tuplestore_putvalues(call_state->tuple_store, call_state->ret_tupdesc,
                               &retval, &isNull);
        }

        pltcl_subtrans_commit(oldcontext, oldowner);
    }
    PG_CATCH();
    {
        pltcl_subtrans_abort(interp, oldcontext, oldowner);
        return TCL_ERROR;
    }
    PG_END_TRY();

    return result;
}
```