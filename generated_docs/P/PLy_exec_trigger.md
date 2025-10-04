# PLy_exec_trigger

## Location
[src/pl/plpython/plpy_exec.c:321-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L321-L434)

## Overview
PLy_exec_trigger is the execution handler for PL/Python trigger functions, managing trigger event processing with support for tuple modification, action control, and proper type conversion between PostgreSQL and Python objects.

## Definition

```c
HeapTuple
PLy_exec_trigger(FunctionCallInfo fcinfo, PLyProcedure *proc)
```
## Detailed Description
This function serves as the core execution handler for PL/Python trigger functions. It handles the complete lifecycle of trigger execution including:

1. **Type System Setup**: Dynamically sets up input/output conversion functions based on the relation's tuple descriptor, handling cases where the relation schema might have changed since the trigger was last called
2. **Trigger Context Management**: Registers trigger data with SPI and builds appropriate trigger arguments for the Python function
3. **Return Value Processing**: Interprets the Python function's return value to control trigger behavior:
   -  or : Accept the tuple as-is
   - : Skip the triggering action
   - : Use the modified tuple (only valid for INSERT/UPDATE triggers)
4. **Tuple Modification**: Handles tuple modification for INSERT and UPDATE triggers when the Python function returns 
5. **Error Handling**: Provides comprehensive validation of return values and proper cleanup

The function expects the Python trigger function to return either None (indicating the tuple is acceptable and unmodified) or a string value indicating the desired action.

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo structure containing the trigger call context and arguments
- `*proc`: PLyProcedure structure containing the compiled Python trigger procedure information
## Dependencies
- Functions called/Symbols referenced:
  - [PLy_global_args_push](PLy_global_args_push.md)/PLy_global_args_pop
  - [PLy_output_setup_func](PLy_output_setup_func.md)/PLy_input_setup_func
  - [PLy_output_setup_tuple](PLy_output_setup_tuple.md)/PLy_input_setup_tuple
  - [PLy_trigger_build_args](PLy_trigger_build_args.md)
  - [PLy_procedure_call](PLy_procedure_call.md)
  - [PLy_modify_tuple](PLy_modify_tuple.md)
  - [SPI_register_trigger_data](../S/SPI_register_trigger_data.md)/SPI_finish
  - CALLED_AS_TRIGGER, TRIGGER_FIRED_BY_* macros
- Called from (representative examples):
  - [plpython3_call_handler](../p/plpython3_call_handler.md) (main trigger handler)

## Notes and Other Information
- Supports all trigger timing (BEFORE/AFTER) and events (INSERT/UPDATE/DELETE)
- Validates return values strictly: None, "OK", "SKIP", or "MODIFY" only
- "MODIFY" return value is ignored (with warning) for DELETE triggers
- Dynamically adapts to relation schema changes by re-setting up type conversion
- Uses PG_FINALLY block to ensure proper cleanup of Python objects and argument stack
- Integrates with PostgreSQL's SPI system for database access within triggers
- File location: src/pl/plpython/plpy_exec.c:321-434

## Simplified Source

```c
HeapTuple
PLy_exec_trigger(FunctionCallInfo fcinfo, PLyProcedure *proc)
{
    HeapTuple rv = NULL;
    PyObject *plargs = NULL;
    PyObject *plrv = NULL;
    TriggerData *tdata;
    TupleDesc rel_descr;

    Assert(CALLED_AS_TRIGGER(fcinfo));
    tdata = (TriggerData *) fcinfo->context;

    // Setup type conversion for the relation's tuple descriptor
    rel_descr = RelationGetDescr(tdata->tg_relation);

    // Setup output conversion if relation type changed
    if (proc->result.typoid != rel_descr->tdtypeid)
        PLy_output_setup_func(&proc->result, proc->mcxt,
                              rel_descr->tdtypeid, rel_descr->tdtypmod, proc);

    // Setup input conversion if relation type changed
    if (proc->result_in.typoid != rel_descr->tdtypeid)
        PLy_input_setup_func(&proc->result_in, proc->mcxt,
                             rel_descr->tdtypeid, rel_descr->tdtypmod, proc);

    // Setup tuple conversion functions
    PLy_output_setup_tuple(&proc->result, rel_descr, proc);
    PLy_input_setup_tuple(&proc->result_in, rel_descr, proc);

    // Handle recursive calls by managing argument stack
    PLy_global_args_push(proc);

    PG_TRY();
    {
        // Register trigger data with SPI
        int rc = SPI_register_trigger_data(tdata);
        Assert(rc >= 0);

        // Build trigger arguments and call Python function
        plargs = PLy_trigger_build_args(fcinfo, proc, &rv);
        plrv = PLy_procedure_call(proc, "TD", plargs);
        Assert(plrv != NULL);

        // Finish SPI connection
        if (SPI_finish() != SPI_OK_FINISH)
            elog(ERROR, "SPI_finish failed");

        // Process return value to determine trigger action
        if (plrv != Py_None) {
            char *srv;

            // Convert return value to string
            if (PyUnicode_Check(plrv)) {
                srv = PLyUnicode_AsString(plrv);
            } else {
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("unexpected return value from trigger procedure"),
                        errdetail("Expected None or a string.")));
                srv = NULL; // keep compiler quiet
            }

            // Handle different return value commands
            if (pg_strcasecmp(srv, "SKIP") == 0) {
                rv = NULL; // Skip the trigger action
            } else if (pg_strcasecmp(srv, "MODIFY") == 0) {
                // Modify tuple for INSERT/UPDATE triggers
                if (TRIGGER_FIRED_BY_INSERT(tdata->tg_event) ||
                    TRIGGER_FIRED_BY_UPDATE(tdata->tg_event)) {
                    rv = PLy_modify_tuple(proc, plargs, tdata, rv);
                } else {
                    ereport(WARNING, (errmsg("PL/Python trigger function returned \"MODIFY\" in a DELETE trigger -- ignored")));
                }
            } else if (pg_strcasecmp(srv, "OK") != 0) {
                // Invalid return value
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("unexpected return value from trigger procedure"),
                        errdetail("Expected None, \"OK\", \"SKIP\", or \"MODIFY\".")));
            }
            // "OK" is treated same as None - no action needed
        }
        // None means accept tuple as-is - no action needed
    }
    PG_FINALLY();
    {
        // Cleanup regardless of success or error
        PLy_global_args_pop(proc);
        Py_XDECREF(plargs);
        Py_XDECREF(plrv);
    }
    PG_END_TRY();

    return rv;
}
```