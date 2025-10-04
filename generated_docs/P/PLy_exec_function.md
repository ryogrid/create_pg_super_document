# PLy_exec_function

## Location
[src/pl/plpython/plpy_exec.c:55-320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L55-L320)

## Overview
PLy_exec_function is the main entry point for executing PL/Python functions and procedures, handling both regular functions and set-returning functions (SRFs) with comprehensive error handling and memory management.

## Definition

```c
Datum
PLy_exec_function(FunctionCallInfo fcinfo, PLyProcedure *proc)
```
## Detailed Description
This function serves as the core execution handler for PL/Python functions and procedures. It manages the complete lifecycle of function execution including:

1. **Argument Management**: Handles recursive function calls by pushing/popping arguments on a global stack
2. **Set-Returning Function Support**: Implements iterator-based processing for functions that return sets of values
3. **Memory Context Management**: Properly manages memory contexts for both regular and SRF execution
4. **Error Handling**: Provides comprehensive error handling with proper cleanup of Python objects and PostgreSQL resources
5. **Type Conversion**: Handles conversion between Python objects and PostgreSQL Datums, including special handling for void, record, and null values
6. **SPI Integration**: Manages SPI (Server Programming Interface) connections for database access

The function uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH) to ensure proper cleanup in case of errors, and implements a state machine approach for set-returning functions to maintain iteration state across multiple calls.

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo structure containing function call context, arguments, and return information
- `*proc`: PLyProcedure structure containing the compiled Python procedure information and metadata
## Dependencies
- Functions called/Symbols referenced:
  - [PLy_global_args_push](PLy_global_args_push.md)/PLy_global_args_pop
  - [PLy_function_build_args](PLy_function_build_args.md)
  - [PLy_procedure_call](PLy_procedure_call.md)
  - [PLy_function_save_args](PLy_function_save_args.md)/PLy_function_restore_args
  - [PLy_output_convert](PLy_output_convert.md)/PLy_output_setup_record
  - SRF_* macros for set-returning function management
  - [SPI_finish](../S/SPI_finish.md) for database connection cleanup
  - PyIter_Next, PyObject_GetIter for Python iteration
- Called from (representative examples):
  - [plpython3_call_handler](../p/plpython3_call_handler.md) (main function handler)
  - [plpython3_inline_handler](../p/plpython3_inline_handler.md) (inline code handler)

## Notes and Other Information
- Supports both regular functions and set-returning functions through a unified interface
- Implements proper cleanup callbacks for SRF state to prevent memory leaks
- Handles special cases for void return types and procedure vs function semantics  
- Uses iterator protocol for efficient handling of large result sets in SRFs
- Maintains argument state across SRF calls to handle interleaved function evaluations
- Integrates with PostgreSQL's error context system for better error reporting
- File location: src/pl/plpython/plpy_exec.c:55-320

## Simplified Source

```c
Datum
PLy_exec_function(FunctionCallInfo fcinfo, PLyProcedure *proc)
{
    bool is_setof = proc->is_setof;
    Datum rv;
    PyObject *plargs = NULL;
    PyObject *plrv = NULL;
    FuncCallContext *funcctx = NULL;
    PLySRFState *srfstate = NULL;
    ErrorContextCallback plerrcontext;

    // Handle recursive calls by managing argument stack
    PLy_global_args_push(proc);

    PG_TRY();
    {
        // Setup for set-returning functions
        if (is_setof) {
            if (SRF_IS_FIRSTCALL()) {
                // Initialize SRF context and state
                funcctx = SRF_FIRSTCALL_INIT();
                srfstate = MemoryContextAllocZero(funcctx->multi_call_memory_ctx,
                                                sizeof(PLySRFState));
                srfstate->callback.func = plpython_srf_cleanup_callback;
                srfstate->callback.arg = srfstate;
                MemoryContextRegisterResetCallback(funcctx->multi_call_memory_ctx,
                                                 &srfstate->callback);
                funcctx->user_fctx = srfstate;
            }
            funcctx = SRF_PERCALL_SETUP();
            srfstate = (PLySRFState *) funcctx->user_fctx;
        }

        // Build arguments and execute function (or restore from saved state)
        if (srfstate == NULL || srfstate->iter == NULL) {
            plargs = PLy_function_build_args(fcinfo, proc);
            plrv = PLy_procedure_call(proc, "args", plargs);
        } else {
            // Restore arguments for continued SRF iteration
            if (srfstate->savedargs)
                PLy_function_restore_args(proc, srfstate->savedargs);
            srfstate->savedargs = NULL;
        }

        // Handle set-returning function iteration
        if (is_setof) {
            if (srfstate->iter == NULL) {
                // First iteration - validate and create iterator
                ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
                if (!rsi || !IsA(rsi, ReturnSetInfo) ||
                    (rsi->allowedModes & SFRM_ValuePerCall) == 0) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("unsupported set function return mode")));
                }
                rsi->returnMode = SFRM_ValuePerCall;
                srfstate->iter = PyObject_GetIter(plrv);
                Py_DECREF(plrv);
                plrv = NULL;

                if (srfstate->iter == NULL)
                    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("returned object cannot be iterated")));
            }

            // Get next value from iterator
            plrv = PyIter_Next(srfstate->iter);
            if (plrv == NULL) {
                // Iterator exhausted or error
                bool has_error = (PyErr_Occurred() != NULL);
                Py_DECREF(srfstate->iter);
                srfstate->iter = NULL;
                if (has_error)
                    PLy_elog(ERROR, "error fetching next item from iterator");
                Py_INCREF(Py_None);
                plrv = Py_None;
            } else {
                // Save arguments for next iteration
                srfstate->savedargs = PLy_function_save_args(proc);
            }
        }

        // Finish SPI connection
        if (SPI_finish() != SPI_OK_FINISH)
            elog(ERROR, "SPI_finish failed");

        // Setup error context for return value conversion
        plerrcontext.callback = plpython_return_error_callback;
        plerrcontext.previous = error_context_stack;
        error_context_stack = &plerrcontext;

        // Convert Python return value to PostgreSQL Datum
        if (proc->result.typoid == VOIDOID) {
            // Handle void return type
            if (plrv != Py_None) {
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("PL/Python %s did not return None",
                               proc->is_procedure ? "procedure" : "function")));
            }
            fcinfo->isnull = false;
            rv = (Datum) 0;
        } else if (plrv == Py_None && srfstate && srfstate->iter == NULL) {
            // SRF iteration ending
            fcinfo->isnull = true;
            rv = (Datum) 0;
        } else {
            // Normal value conversion
            if (proc->result.typoid == RECORDOID) {
                TupleDesc desc;
                if (get_call_result_type(fcinfo, NULL, &desc) != TYPEFUNC_COMPOSITE)
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("function returning record called in invalid context")));
                PLy_output_setup_record(&proc->result, desc, proc);
            }
            rv = PLy_output_convert(&proc->result, plrv, &fcinfo->isnull);
        }
    }
    PG_CATCH();
    {
        // Cleanup on error
        PLy_global_args_pop(proc);
        Py_XDECREF(plargs);
        Py_XDECREF(plrv);

        if (srfstate) {
            Py_XDECREF(srfstate->iter);
            srfstate->iter = NULL;
            if (srfstate->savedargs)
                PLy_function_drop_args(srfstate->savedargs);
            srfstate->savedargs = NULL;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Normal cleanup
    error_context_stack = plerrcontext.previous;
    PLy_global_args_pop(proc);
    Py_XDECREF(plargs);
    Py_DECREF(plrv);

    // Return appropriate result based on function type
    if (srfstate) {
        if (srfstate->iter == NULL)
            SRF_RETURN_DONE(funcctx);
        else if (fcinfo->isnull)
            SRF_RETURN_NEXT_NULL(funcctx);
        else
            SRF_RETURN_NEXT(funcctx, rv);
    }

    return rv;
}
```