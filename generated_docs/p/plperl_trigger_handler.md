# plperl_trigger_handler

## Location
[src/pl/plperl/plperl.c:2521-2633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2521-L2633)

## Overview
This function serves as the main handler for PL/Perl trigger functions, managing trigger execution, result processing, and row modification based on trigger return values.

## Definition
```c
static Datum plperl_trigger_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
`plperl_trigger_handler` is the primary entry point for executing PL/Perl trigger functions in PostgreSQL. It manages the complete trigger execution lifecycle including SPI connection with transition table registration, trigger data preparation, function compilation and execution, and result interpretation. The function handles the PostgreSQL trigger protocol where return values control trigger behavior: undef (proceed with original operation), "SKIP" (cancel operation), or "MODIFY" (use modified row data).

The function builds the `$_TD` hash containing trigger metadata and row data, executes the Perl trigger function, and processes the return value to determine the appropriate action. For row modification, it converts the Perl hash back to a PostgreSQL HeapTuple.

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro providing access to:
  - `fcinfo`: FunctionCallInfo containing trigger context and metadata
  - `fcinfo->context`: Cast to TriggerData containing trigger-specific information

## Dependencies
- Functions called/Symbols referenced:
  - [SPI_connect](../S/SPI_connect.md) / SPI_finish (SPI connection management)
  - [SPI_register_trigger_data](../S/SPI_register_trigger_data.md) (transition table support)
  - [compile_plperl_function](../c/compile_plperl_function.md) (function compilation)
  - increment_prodesc_refcount (reference counting)
  - [activate_interpreter](../a/activate_interpreter.md) (Perl interpreter management)
  - [plperl_trigger_build_args](plperl_trigger_build_args.md) (builds $_TD hash)
  - [plperl_call_perl_trigger_func](plperl_call_perl_trigger_func.md) (executes Perl code)
  - [plperl_modify_tuple](plperl_modify_tuple.md) (converts Perl hash to HeapTuple)
  - [sv2cstr](../s/sv2cstr.md) (string conversion)
  - [plperl_exec_callback](plperl_exec_callback.md) (error context)
- Called from:
  - [plperl_call_handler](plperl_call_handler.md)

## Notes and Other Information
- Registers transition tables with SPI for NEW/OLD table access in trigger functions
- Supports all trigger types: INSERT, UPDATE, DELETE, TRUNCATE
- Implements PostgreSQL trigger protocol with three return value interpretations:
  - undef/NULL: proceed with original tuple
  - "SKIP": cancel the triggering operation
  - "MODIFY": use modified row data from $_TD hash
- Handles row modification only for INSERT and UPDATE triggers (warns for DELETE)
- Provides comprehensive error handling with meaningful error messages
- Manages Perl reference counting for proper memory cleanup
- Uses error context callbacks for better error reporting with function names
- Validates trigger return values and provides clear error messages for invalid returns

## Simplified Source

```c
static Datum plperl_trigger_handler(PG_FUNCTION_ARGS) {
    plperl_proc_desc *prodesc;
    SV *perlret;
    Datum retval = (Datum) 0;
    SV *svTD;
    HV *hvTD;
    ErrorContextCallback pl_error_context;
    TriggerData *tdata = (TriggerData *) fcinfo->context;
    char *stroid;

    // Connect to SPI and register trigger data for transition tables
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "could not connect to SPI manager");
    SPI_register_trigger_data(tdata);

    // Compile trigger function and set up execution context
    prodesc = compile_plperl_function(fcinfo->flinfo->fn_oid, true, false);
    current_call_data->prodesc = prodesc;
    increment_prodesc_refcount(prodesc);

    // Set up error reporting context
    pl_error_context.callback = plperl_exec_callback;
    pl_error_context.previous = error_context_stack;
    pl_error_context.arg = prodesc->proname;
    error_context_stack = &pl_error_context;

    activate_interpreter(prodesc->interp);

    // Build $_TD hash with trigger data
    svTD = plperl_trigger_build_args(fcinfo);
    perlret = plperl_call_perl_trigger_func(prodesc, fcinfo, svTD);
    hvTD = (HV *) SvRV(svTD);

    // Process trigger return value
    if (perlret == NULL || !SvOK(perlret)) {
        // undef return: proceed with original operation
        retval = (Datum) tdata->tg_trigtuple;
    } else if (SvROK(perlret)) {
        // Hash reference: modify the tuple
        HV *hvNew = (HV *) SvRV(perlret);
        HeapTuple trv;

        if (SvTYPE(hvNew) != SVt_PVHV)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("trigger \"'%s'\" for table \"'%s'\" failed: "
                                  "function returned reference to %s instead of reference to hash",
                                  prodesc->proname,
                                  RelationGetRelationName(tdata->tg_relation),
                                  sv_reftype(SvRV(perlret), 0))));

        // Build modified tuple from Perl hash
        trv = plperl_modify_tuple(hvTD, tdata, retval);
        retval = (Datum) trv;
    } else {
        // String return: check for "SKIP"
        stroid = sv2cstr(perlret);
        if (strcmp(stroid, "SKIP") == 0)
            retval = (Datum) NULL;  // Skip operation
        else if (strcmp(stroid, "MODIFY") == 0) {
            // Use modified data from $_TD hash
            SV **svp = hv_fetch_string(hvTD, "new");
            HV *hvNew;

            if (!svp)
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                               errmsg("$_TD->{new} does not exist")));
            if (!SvOK(*svp) || !SvROK(*svp) || SvTYPE(SvRV(*svp)) != SVt_PVHV)
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                               errmsg("$_TD->{new} is not a hash reference")));

            hvNew = (HV *) SvRV(*svp);
            retval = (Datum) plperl_modify_tuple(hvTD, tdata, retval);
        } else {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("result of PL/Perl trigger function must be undef, "
                                  "\"'SKIP'\", \"'MODIFY'\", or reference to hash")));
        }
        pfree(stroid);
    }

    // Cleanup and return
    error_context_stack = pl_error_context.previous;
    SvREFCNT_dec_current(perlret);
    SvREFCNT_dec_current(svTD);

    if (SPI_finish() != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish() failed");

    return retval;
}
```