# pltcl_func_handler

## Location
[src/pl/tcl/pltcl.c:797-1055](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L797-L1055)

## Overview
Handles regular function calls for PL/Tcl, managing argument conversion, Tcl function execution, and result processing for both scalar and set-returning functions.

## Definition
```c
static Datum pltcl_func_handler(PG_FUNCTION_ARGS, pltcl_call_state *call_state, bool pltrusted)
```

## Detailed Description
`pltcl_func_handler` is the core function execution handler for PL/Tcl that processes regular (non-trigger) function calls. It performs the complete lifecycle of function execution: establishing SPI connection, compiling/finding the function, converting PostgreSQL arguments to Tcl format, executing the Tcl function, and converting results back to PostgreSQL format.

The function handles multiple return types including scalars, tuples, and set-returning functions. It manages proper memory contexts, reference counting, and exception handling throughout the execution process. For set-returning functions, it supports materialized tuple stores. For composite types, it handles both named composite types and RECORD types with dynamic structure determination.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing arguments and call context
- `call_state`: Pointer to pltcl_call_state structure tracking execution state and resources
- `pltrusted`: Boolean flag indicating whether to operate in trusted (true) or untrusted (false) mode

## Dependencies
- Functions called/Symbols referenced:
  - [SPI_connect_ext](../S/SPI_connect_ext.md)/SPI_finish (SPI interface management)
  - [compile_pltcl_function](../c/compile_pltcl_function.md) (function compilation/lookup)
  - [pltcl_build_tuple_argument](pltcl_build_tuple_argument.md) (tuple to Tcl conversion)
  - [pltcl_build_tuple_result](pltcl_build_tuple_result.md) (Tcl to tuple conversion)
  - [OutputFunctionCall](../O/OutputFunctionCall.md)/InputFunctionCall (data type I/O)
  - HeapTupleHeader functions (tuple manipulation)
  - [TupleDescGetAttInMetadata](../T/TupleDescGetAttInMetadata.md) (tuple descriptor utilities)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md) (tuple descriptor copying)
  - [throw_tcl_error](../t/throw_tcl_error.md) (error handling)
  - Tcl library functions (Tcl_EvalObjEx, Tcl_ListObjAppendElement, etc.)
- Called from (representative examples):
  - [pltcl_handler](pltcl_handler.md) (main dispatcher)

## Notes and Other Information
- This is a static function, not directly accessible outside the PL/Tcl module
- Supports both atomic and non-atomic execution contexts
- Handles NULL arguments and return values appropriately
- Implements proper UTF-8 encoding conversion between PostgreSQL and Tcl
- Manages reference counting for Tcl objects to prevent memory leaks
- Supports complex argument types including row types and domains
- For set-returning functions, uses materialized tuple stores in the caller's memory context
- Implements comprehensive error handling with proper resource cleanup
- Handles both predetermined composite return types and dynamic RECORD types

## Simplified Source

```c
static Datum
pltcl_func_handler(PG_FUNCTION_ARGS, pltcl_call_state *call_state, bool pltrusted)
{
    bool nonatomic;
    pltcl_proc_desc *prodesc;
    Tcl_Interp *interp;
    Tcl_Obj *tcl_cmd;
    int tcl_rc;
    Datum retval;

    // Determine execution context atomicity
    nonatomic = fcinfo->context && IsA(fcinfo->context, CallContext) &&
               !castNode(CallContext, fcinfo->context)->atomic;

    // Connect to SPI with appropriate mode
    if (SPI_connect_ext(nonatomic ? SPI_OPT_NONATOMIC : 0) != SPI_OK_CONNECT)
        elog(ERROR, "could not connect to SPI manager");

    // Find or compile the function
    prodesc = compile_pltcl_function(fcinfo->flinfo->fn_oid, InvalidOid, false, pltrusted);
    call_state->prodesc = prodesc;
    prodesc->fn_refcount++;
    interp = prodesc->interp_desc->interp;

    // Handle set-returning functions setup
    if (prodesc->fn_retisset) {
        ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
        if (!rsi || !IsA(rsi, ReturnSetInfo) || !(rsi->allowedModes & SFRM_Materialize))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("materialize mode required, but it is not allowed in this context")));

        call_state->rsi = rsi;
        call_state->tuple_store_cxt = rsi->econtext->ecxt_per_query_memory;
        call_state->tuple_store_owner = CurrentResourceOwner;
    }

    // Build Tcl command with function name and arguments
    tcl_cmd = Tcl_NewObj();
    Tcl_IncrRefCount(tcl_cmd);
    Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj(prodesc->internal_proname, -1));

    PG_TRY();
    {
        // Convert and add all arguments to the command
        for (int i = 0; i < prodesc->nargs; i++) {
            if (fcinfo->args[i].isnull) {
                Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewObj());
            }
            else if (prodesc->arg_is_rowtype[i]) {
                // Handle tuple arguments
                HeapTupleHeader td = DatumGetHeapTupleHeader(fcinfo->args[i].value);
                Oid tupType = HeapTupleHeaderGetTypeId(td);
                int32 tupTypmod = HeapTupleHeaderGetTypMod(td);
                TupleDesc tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
                HeapTupleData tmptup;
                tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
                tmptup.t_data = td;

                Tcl_Obj *list_tmp = pltcl_build_tuple_argument(&tmptup, tupdesc, true);
                Tcl_ListObjAppendElement(NULL, tcl_cmd, list_tmp);
                ReleaseTupleDesc(tupdesc);
            }
            else {
                // Handle scalar arguments
                char *tmp = OutputFunctionCall(&prodesc->arg_out_func[i], fcinfo->args[i].value);
                Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj(UTF_E2U(tmp), -1));
                pfree(tmp);
            }
        }
    }
    PG_CATCH();
    {
        Tcl_DecrRefCount(tcl_cmd);
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Execute the Tcl function
    tcl_rc = Tcl_EvalObjEx(interp, tcl_cmd, (TCL_EVAL_DIRECT | TCL_EVAL_GLOBAL));
    Tcl_DecrRefCount(tcl_cmd);

    if (tcl_rc != TCL_OK)
        throw_tcl_error(interp, prodesc->user_proname);

    // Disconnect from SPI
    if (SPI_finish() != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish() failed");

    // Process return value based on function type
    if (prodesc->fn_retisset) {
        // Set-returning function
        ReturnSetInfo *rsi = call_state->rsi;
        rsi->returnMode = SFRM_Materialize;
        if (call_state->tuple_store) {
            rsi->setResult = call_state->tuple_store;
            if (call_state->ret_tupdesc) {
                MemoryContext oldcxt = MemoryContextSwitchTo(call_state->tuple_store_cxt);
                rsi->setDesc = CreateTupleDescCopy(call_state->ret_tupdesc);
                MemoryContextSwitchTo(oldcxt);
            }
        }
        retval = (Datum) 0;
        fcinfo->isnull = true;
    }
    else if (fcinfo->isnull) {
        retval = InputFunctionCall(&prodesc->result_in_func, NULL,
                                  prodesc->result_typioparam, -1);
    }
    else if (prodesc->fn_retistuple) {
        // Tuple return type
        TupleDesc td;
        HeapTuple tup;
        Tcl_Obj *resultObj = Tcl_GetObjResult(interp);
        Tcl_Obj **resultObjv;
        Tcl_Size resultObjc;

        switch (get_call_result_type(fcinfo, NULL, &td)) {
            case TYPEFUNC_COMPOSITE:
            case TYPEFUNC_COMPOSITE_DOMAIN:
                break;
            case TYPEFUNC_RECORD:
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("function returning record called in context that cannot accept type record")));
                break;
            default:
                elog(ERROR, "return type must be a row type");
                break;
        }

        call_state->ret_tupdesc = td;
        call_state->attinmeta = TupleDescGetAttInMetadata(td);

        if (Tcl_ListObjGetElements(interp, resultObj, &resultObjc, &resultObjv) == TCL_ERROR)
            ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                           errmsg("could not parse function return value: %s",
                                  utf_u2e(Tcl_GetStringResult(interp)))));

        tup = pltcl_build_tuple_result(interp, resultObjv, resultObjc, call_state);
        retval = HeapTupleGetDatum(tup);
    }
    else {
        // Scalar return type
        retval = InputFunctionCall(&prodesc->result_in_func,
                                  utf_u2e(Tcl_GetStringResult(interp)),
                                  prodesc->result_typioparam, -1);
    }

    return retval;
}
```