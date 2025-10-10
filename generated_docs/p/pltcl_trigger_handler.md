# pltcl_trigger_handler

## Location
[src/pl/tcl/pltcl.c:1056-1315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L1056-L1315)

## Overview
Handles trigger calls for PL/Tcl, managing trigger context setup, argument conversion, Tcl trigger function execution, and result processing for both row and statement-level triggers.

## Definition
```c
static HeapTuple pltcl_trigger_handler(PG_FUNCTION_ARGS, pltcl_call_state *call_state, bool pltrusted)
```

## Detailed Description
`pltcl_trigger_handler` is the specialized trigger execution handler for PL/Tcl that processes database trigger calls. It extracts trigger context information (trigger name, relation details, event type, timing, level), converts PostgreSQL trigger data to Tcl format, executes the Tcl trigger function, and processes the return value to determine trigger behavior.

The function handles both row-level and statement-level triggers across all trigger events (INSERT, UPDATE, DELETE, TRUNCATE) and timings (BEFORE, AFTER, INSTEAD OF). It provides comprehensive trigger context to the Tcl function including trigger metadata, relation information, OLD/NEW tuple data for row triggers, and user-defined trigger arguments. The function supports trigger transition tables and manages proper memory context and exception handling throughout execution.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing trigger context
- `call_state`: Pointer to pltcl_call_state structure tracking execution state and trigger data
- `pltrusted`: Boolean flag indicating whether to operate in trusted (true) or untrusted (false) mode

## Dependencies
- Functions called/Symbols referenced:
  - [SPI_connect](../S/SPI_connect.md)/SPI_finish (SPI interface management)
  - [SPI_register_trigger_data](../S/SPI_register_trigger_data.md) (transition table registration)
  - [compile_pltcl_function](../c/compile_pltcl_function.md) (function compilation/lookup)
  - [pltcl_build_tuple_argument](pltcl_build_tuple_argument.md) (tuple to Tcl conversion)
  - [pltcl_build_tuple_result](pltcl_build_tuple_result.md) (Tcl to tuple conversion)
  - [TriggerData](../T/TriggerData.md) structure and related macros (trigger context)
  - DirectFunctionCall1/oidout (OID conversion)
  - [SPI_getrelname](../S/SPI_getrelname.md)/SPI_getnspname (relation metadata)
  - TRIGGER_FIRED_* macros (trigger event detection)
  - [throw_tcl_error](../t/throw_tcl_error.md) (error handling)
  - [utf_e2u](../u/utf_e2u.md)/utf_u2e (encoding conversion)
  - Tcl library functions (Tcl_EvalObjEx, Tcl_ListObjAppendElement, etc.)
- Called from (representative examples):
  - [pltcl_handler](pltcl_handler.md) (main dispatcher)

## Notes and Other Information
- This is a static function, not directly accessible outside the PL/Tcl module
- Returns HeapTuple (modified tuple for row triggers) or NULL (to skip trigger action)
- Supports magic return values "OK" (return original tuple) and "SKIP" (return NULL)
- Handles stored generated columns properly (excludes them from BEFORE trigger NEW rows)
- Provides comprehensive trigger context to Tcl functions including:
  - TG_name (trigger name)
  - TG_relid (relation OID)
  - TG_table_name (table name)
  - TG_table_schema (schema name)
  - TG_relatts (attribute names)
  - TG_when (BEFORE/AFTER/INSTEAD OF)
  - TG_level (ROW/STATEMENT)
  - TG_op (INSERT/UPDATE/DELETE/TRUNCATE)
  - NEW and OLD tuple data (for row triggers)
  - User-defined trigger arguments
- Supports both row-level and statement-level triggers with appropriate context
- Implements proper exception handling with resource cleanup
- Manages transition table visibility for complex trigger scenarios

## Simplified Source

```c
static HeapTuple
pltcl_trigger_handler(PG_FUNCTION_ARGS, pltcl_call_state *call_state, bool pltrusted)
{
    pltcl_proc_desc *prodesc;
    Tcl_Interp *interp;
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    TupleDesc tupdesc;
    volatile HeapTuple rettup;
    Tcl_Obj *tcl_cmd;
    int tcl_rc;
    const char *result;

    call_state->trigdata = trigdata;

    // Connect to SPI and register trigger data
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "could not connect to SPI manager");
    SPI_register_trigger_data(trigdata);

    // Find or compile the trigger function
    prodesc = compile_pltcl_function(fcinfo->flinfo->fn_oid,
                                    RelationGetRelid(trigdata->tg_relation),
                                    false, pltrusted);
    call_state->prodesc = prodesc;
    prodesc->fn_refcount++;
    interp = prodesc->interp_desc->interp;
    tupdesc = RelationGetDescr(trigdata->tg_relation);

    // Build Tcl command with trigger context
    tcl_cmd = Tcl_NewObj();
    Tcl_IncrRefCount(tcl_cmd);

    PG_TRY();
    {
        // Add procedure name
        Tcl_ListObjAppendElement(NULL, tcl_cmd,
                                Tcl_NewStringObj(prodesc->internal_proname, -1));

        // Add trigger name (TG_name)
        Tcl_ListObjAppendElement(NULL, tcl_cmd,
                                Tcl_NewStringObj(utf_e2u(trigdata->tg_trigger->tgname), -1));

        // Add relation OID (TG_relid)
        char *stroid = DatumGetCString(DirectFunctionCall1(oidout,
                                      ObjectIdGetDatum(trigdata->tg_relation->rd_id)));
        Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj(stroid, -1));
        pfree(stroid);

        // Add table name (TG_table_name)
        stroid = SPI_getrelname(trigdata->tg_relation);
        Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj(utf_e2u(stroid), -1));
        pfree(stroid);

        // Add schema name (TG_table_schema)
        stroid = SPI_getnspname(trigdata->tg_relation);
        Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj(utf_e2u(stroid), -1));
        pfree(stroid);

        // Add attribute names list (TG_relatts)
        Tcl_Obj *tcl_trigtup = Tcl_NewObj();
        Tcl_ListObjAppendElement(NULL, tcl_trigtup, Tcl_NewObj());
        for (int i = 0; i < tupdesc->natts; i++) {
            Form_pg_attribute att = TupleDescAttr(tupdesc, i);
            if (att->attisdropped)
                Tcl_ListObjAppendElement(NULL, tcl_trigtup, Tcl_NewObj());
            else
                Tcl_ListObjAppendElement(NULL, tcl_trigtup,
                                        Tcl_NewStringObj(utf_e2u(NameStr(att->attname)), -1));
        }
        Tcl_ListObjAppendElement(NULL, tcl_cmd, tcl_trigtup);

        // Add timing (TG_when): BEFORE/AFTER/INSTEAD OF
        if (TRIGGER_FIRED_BEFORE(trigdata->tg_event))
            Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("BEFORE", -1));
        else if (TRIGGER_FIRED_AFTER(trigdata->tg_event))
            Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("AFTER", -1));
        else if (TRIGGER_FIRED_INSTEAD(trigdata->tg_event))
            Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("INSTEAD OF", -1));

        // Add level and operation-specific data
        if (TRIGGER_FIRED_FOR_ROW(trigdata->tg_event)) {
            Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("ROW", -1));

            // Add operation and tuple data
            if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event)) {
                Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("INSERT", -1));
                Tcl_ListObjAppendElement(NULL, tcl_cmd,
                                        pltcl_build_tuple_argument(trigdata->tg_trigtuple, tupdesc,
                                                                   !TRIGGER_FIRED_BEFORE(trigdata->tg_event)));
                Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewObj());
                rettup = trigdata->tg_trigtuple;
            }
            else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)) {
                Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("DELETE", -1));
                Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewObj());
                Tcl_ListObjAppendElement(NULL, tcl_cmd,
                                        pltcl_build_tuple_argument(trigdata->tg_trigtuple, tupdesc, true));
                rettup = trigdata->tg_trigtuple;
            }
            else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)) {
                Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("UPDATE", -1));
                Tcl_ListObjAppendElement(NULL, tcl_cmd,
                                        pltcl_build_tuple_argument(trigdata->tg_newtuple, tupdesc,
                                                                   !TRIGGER_FIRED_BEFORE(trigdata->tg_event)));
                Tcl_ListObjAppendElement(NULL, tcl_cmd,
                                        pltcl_build_tuple_argument(trigdata->tg_trigtuple, tupdesc, true));
                rettup = trigdata->tg_newtuple;
            }
        }
        else if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event)) {
            Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("STATEMENT", -1));

            // Add operation name
            if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
                Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("INSERT", -1));
            else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
                Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("DELETE", -1));
            else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
                Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("UPDATE", -1));
            else if (TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event))
                Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewStringObj("TRUNCATE", -1));

            Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewObj());
            Tcl_ListObjAppendElement(NULL, tcl_cmd, Tcl_NewObj());
            rettup = (HeapTuple) NULL;
        }

        // Add trigger arguments
        for (int i = 0; i < trigdata->tg_trigger->tgnargs; i++)
            Tcl_ListObjAppendElement(NULL, tcl_cmd,
                                    Tcl_NewStringObj(utf_e2u(trigdata->tg_trigger->tgargs[i]), -1));
    }
    PG_CATCH();
    {
        Tcl_DecrRefCount(tcl_cmd);
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Execute the Tcl trigger function
    tcl_rc = Tcl_EvalObjEx(interp, tcl_cmd, (TCL_EVAL_DIRECT | TCL_EVAL_GLOBAL));
    Tcl_DecrRefCount(tcl_cmd);

    if (tcl_rc != TCL_OK)
        throw_tcl_error(interp, prodesc->user_proname);

    if (SPI_finish() != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish() failed");

    // Process trigger return value
    result = Tcl_GetStringResult(interp);

    if (strcmp(result, "OK") == 0)
        return rettup;
    if (strcmp(result, "SKIP") == 0)
        return (HeapTuple) NULL;

    // Parse custom tuple return value
    Tcl_Size result_Objc;
    Tcl_Obj **result_Objv;
    if (Tcl_ListObjGetElements(interp, Tcl_GetObjResult(interp),
                              &result_Objc, &result_Objv) != TCL_OK)
        ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                       errmsg("could not parse trigger return value: %s",
                              utf_u2e(Tcl_GetStringResult(interp)))));

    rettup = pltcl_build_tuple_result(interp, result_Objv, result_Objc, call_state);
    return rettup;
}
```