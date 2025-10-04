# PLy_trigger_build_args

## Location
[src/pl/plpython/plpy_exec.c:705-921](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L705-L921)

## Overview
Builds a Python dictionary containing trigger-related information and arguments for PL/Python trigger functions, providing access to trigger context, table metadata, old/new tuples, and trigger arguments.

## Definition

```c
static PyObject *
PLy_trigger_build_args(FunctionCallInfo fcinfo, PLyProcedure *proc, HeapTuple *rv)
```
## Detailed Description
This function constructs a comprehensive Python dictionary that contains all the information needed by a PL/Python trigger function. It extracts trigger metadata from the TriggerData structure and converts PostgreSQL data types to their Python equivalents. The function handles different trigger types (BEFORE/AFTER/INSTEAD OF), trigger levels (ROW/STATEMENT), and trigger events (INSERT/DELETE/UPDATE/TRUNCATE). For row-level triggers, it converts the old and new tuples to Python objects, while for statement-level triggers, it sets these to None. The function also handles trigger arguments and provides table metadata such as relation ID, table name, and schema name.

## Parameters / Member Variables
- `fcinfo`: Function call information containing the trigger context data
- `*proc`: PL/Python procedure information including input conversion functions
- `*rv`: Output parameter that receives the HeapTuple to be returned by the trigger
## Dependencies
- Functions called/Symbols referenced:
  - [PLyUnicode_FromString](PLyUnicode_FromString.md)
  - [PLy_input_from_tuple](PLy_input_from_tuple.md)
  - DirectFunctionCall1
  - [DatumGetCString](../D/DatumGetCString.md)
  - [SPI_getrelname](../S/SPI_getrelname.md)
  - [SPI_getnspname](../S/SPI_getnspname.md)
  - TRIGGER_FIRED_BEFORE/AFTER/INSTEAD
  - TRIGGER_FIRED_FOR_ROW/STATEMENT
  - TRIGGER_FIRED_BY_INSERT/DELETE/UPDATE/TRUNCATE
- Called from (representative examples):
  - [PLy_exec_trigger](PLy_exec_trigger.md)

## Notes and Other Information
The function creates a Python dictionary with the following keys:
- 'name': Trigger name
- 'relid': Relation OID as string
- 'table_name': Table name
- 'table_schema': Schema name  
- 'when': Trigger timing ('BEFORE', 'AFTER', 'INSTEAD OF')
- 'level': Trigger level ('ROW', 'STATEMENT')
- 'event': Trigger event ('INSERT', 'DELETE', 'UPDATE', 'TRUNCATE')
- 'old': Old tuple for row-level triggers (None for statement-level)
- 'new': New tuple for row-level triggers (None for statement-level)
- 'args': List of trigger arguments

The function uses PG_TRY/PG_CATCH blocks for proper error handling and Python reference counting. For BEFORE triggers on row-level operations, stored generated columns are not included in the NEW tuple as they haven't been computed yet.

## Simplified Source

```c
static PyObject *
PLy_trigger_build_args(FunctionCallInfo fcinfo, PLyProcedure *proc, HeapTuple *rv)
{
    TriggerData *tdata = (TriggerData *) fcinfo->context;
    TupleDesc rel_descr = RelationGetDescr(tdata->tg_relation);
    PyObject *pltdata, *pltargs;

    // Create the main trigger data dictionary
    pltdata = PyDict_New();
    if (!pltdata)
        return NULL;

    // Handle trigger arguments
    if (tdata->tg_trigger->tgnargs) {
        pltargs = PyList_New(tdata->tg_trigger->tgnargs);
        if (!pltargs) {
            Py_DECREF(pltdata);
            return NULL;
        }
    } else {
        Py_INCREF(Py_None);
        pltargs = Py_None;
    }

    PG_TRY();
    {
        // Set basic trigger information
        PyDict_SetItemString(pltdata, "name",
                           PLyUnicode_FromString(tdata->tg_trigger->tgname));

        // Set table metadata
        char *relid_str = DatumGetCString(DirectFunctionCall1(oidout,
                                ObjectIdGetDatum(tdata->tg_relation->rd_id)));
        PyDict_SetItemString(pltdata, "relid", PLyUnicode_FromString(relid_str));
        pfree(relid_str);

        char *table_name = SPI_getrelname(tdata->tg_relation);
        PyDict_SetItemString(pltdata, "table_name", PLyUnicode_FromString(table_name));
        pfree(table_name);

        char *schema_name = SPI_getnspname(tdata->tg_relation);
        PyDict_SetItemString(pltdata, "table_schema", PLyUnicode_FromString(schema_name));
        pfree(schema_name);

        // Set trigger timing (when)
        if (TRIGGER_FIRED_BEFORE(tdata->tg_event))
            PyDict_SetItemString(pltdata, "when", PLyUnicode_FromString("BEFORE"));
        else if (TRIGGER_FIRED_AFTER(tdata->tg_event))
            PyDict_SetItemString(pltdata, "when", PLyUnicode_FromString("AFTER"));
        else if (TRIGGER_FIRED_INSTEAD(tdata->tg_event))
            PyDict_SetItemString(pltdata, "when", PLyUnicode_FromString("INSTEAD OF"));

        // Handle row-level vs statement-level triggers
        if (TRIGGER_FIRED_FOR_ROW(tdata->tg_event)) {
            PyDict_SetItemString(pltdata, "level", PLyUnicode_FromString("ROW"));

            // Handle different trigger events for row-level
            if (TRIGGER_FIRED_BY_INSERT(tdata->tg_event)) {
                PyDict_SetItemString(pltdata, "event", PLyUnicode_FromString("INSERT"));
                PyDict_SetItemString(pltdata, "old", Py_None);
                PyObject *pytnew = PLy_input_from_tuple(&proc->result_in,
                                                       tdata->tg_trigtuple, rel_descr,
                                                       !TRIGGER_FIRED_BEFORE(tdata->tg_event));
                PyDict_SetItemString(pltdata, "new", pytnew);
                *rv = tdata->tg_trigtuple;
            }
            else if (TRIGGER_FIRED_BY_DELETE(tdata->tg_event)) {
                PyDict_SetItemString(pltdata, "event", PLyUnicode_FromString("DELETE"));
                PyDict_SetItemString(pltdata, "new", Py_None);
                PyObject *pytold = PLy_input_from_tuple(&proc->result_in,
                                                       tdata->tg_trigtuple, rel_descr, true);
                PyDict_SetItemString(pltdata, "old", pytold);
                *rv = tdata->tg_trigtuple;
            }
            else if (TRIGGER_FIRED_BY_UPDATE(tdata->tg_event)) {
                PyDict_SetItemString(pltdata, "event", PLyUnicode_FromString("UPDATE"));
                PyObject *pytnew = PLy_input_from_tuple(&proc->result_in,
                                                       tdata->tg_newtuple, rel_descr,
                                                       !TRIGGER_FIRED_BEFORE(tdata->tg_event));
                PyDict_SetItemString(pltdata, "new", pytnew);
                PyObject *pytold = PLy_input_from_tuple(&proc->result_in,
                                                       tdata->tg_trigtuple, rel_descr, true);
                PyDict_SetItemString(pltdata, "old", pytold);
                *rv = tdata->tg_newtuple;
            }
        }
        else if (TRIGGER_FIRED_FOR_STATEMENT(tdata->tg_event)) {
            PyDict_SetItemString(pltdata, "level", PLyUnicode_FromString("STATEMENT"));
            PyDict_SetItemString(pltdata, "old", Py_None);
            PyDict_SetItemString(pltdata, "new", Py_None);
            *rv = NULL;

            // Set event type for statement-level triggers
            if (TRIGGER_FIRED_BY_INSERT(tdata->tg_event))
                PyDict_SetItemString(pltdata, "event", PLyUnicode_FromString("INSERT"));
            else if (TRIGGER_FIRED_BY_DELETE(tdata->tg_event))
                PyDict_SetItemString(pltdata, "event", PLyUnicode_FromString("DELETE"));
            else if (TRIGGER_FIRED_BY_UPDATE(tdata->tg_event))
                PyDict_SetItemString(pltdata, "event", PLyUnicode_FromString("UPDATE"));
            else if (TRIGGER_FIRED_BY_TRUNCATE(tdata->tg_event))
                PyDict_SetItemString(pltdata, "event", PLyUnicode_FromString("TRUNCATE"));
        }

        // Add trigger arguments
        if (tdata->tg_trigger->tgnargs) {
            for (int i = 0; i < tdata->tg_trigger->tgnargs; i++) {
                PyObject *pltarg = PLyUnicode_FromString(tdata->tg_trigger->tgargs[i]);
                PyList_SetItem(pltargs, i, pltarg);
            }
        }
        PyDict_SetItemString(pltdata, "args", pltargs);
    }
    PG_CATCH();
    {
        Py_XDECREF(pltargs);
        Py_XDECREF(pltdata);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return pltdata;
}
```