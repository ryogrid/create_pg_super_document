# PLy_modify_tuple

## Location
[src/pl/plpython/plpy_exec.c:922-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L922-L1051)

## Overview
Applies row modifications requested by a PL/Python trigger function by converting Python dictionary changes back to PostgreSQL tuple format and creating a modified HeapTuple.

## Definition

```c
static HeapTuple
PLy_modify_tuple(PLyProcedure *proc, PyObject *pltd, TriggerData *tdata,
				 HeapTuple otup)
```
## Detailed Description
This function processes modifications to a row tuple as specified by a PL/Python trigger function. It extracts the 'new' dictionary from the trigger data (pltd), validates its structure, and applies the changes to create a new HeapTuple. The function iterates through all keys in the 'new' dictionary, validates that they correspond to valid, modifiable table columns, converts Python values to PostgreSQL Datums using the appropriate conversion functions, and constructs arrays for values, nulls, and replacement flags. It prevents modification of system attributes, generated columns, and validates column existence. The function uses PostgreSQL's heap_modify_tuple to create the final modified tuple.

## Parameters / Member Variables
- `*proc`: PL/Python procedure containing result conversion information and attribute details
- `*pltd`: Python trigger data dictionary containing the 'new' key with modified values
- `*tdata`: Trigger context data including relation information and tuple descriptors
- `otup`: Original HeapTuple to be modified
## Dependencies
- Functions called/Symbols referenced:
  - [plpython_trigger_error_callback](../p/plpython_trigger_error_callback.md)
  - [PLyUnicode_AsString](PLyUnicode_AsString.md)
  - [SPI_fnumber](../S/SPI_fnumber.md)
  - [PLy_output_convert](PLy_output_convert.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - TupleDescAttr
  - RelationGetDescr
  - PyDict_GetItemString
  - PyDict_Keys
  - PyList_Size
- Called from (representative examples):
  - [PLy_exec_trigger](PLy_exec_trigger.md)

## Notes and Other Information
The function performs extensive validation including:
- Ensuring the 'new' key exists in the trigger data dictionary
- Verifying 'new' is a dictionary object
- Checking that all dictionary keys are strings representing valid column names
- Preventing modification of system attributes (attn <= 0)
- Blocking changes to generated columns
- Validating column existence in the table schema

The function uses error context callbacks for better error reporting and PG_TRY/PG_CATCH blocks for proper resource cleanup. Memory is allocated for modvalues, modnulls, and modrepls arrays which track the new values, null status, and which columns should be replaced. These are automatically freed in both success and error paths.

## Simplified Source

```c
static HeapTuple
PLy_modify_tuple(PLyProcedure *proc, PyObject *pltd, TriggerData *tdata, HeapTuple otup)
{
    HeapTuple rtup;
    PyObject *plntup, *plkeys, *plval = NULL;
    Datum *modvalues;
    bool *modnulls, *modrepls;
    ErrorContextCallback plerrcontext;

    // Set up error context for better error reporting
    plerrcontext.callback = plpython_trigger_error_callback;
    plerrcontext.previous = error_context_stack;
    error_context_stack = &plerrcontext;

    PG_TRY();
    {
        TupleDesc tupdesc = RelationGetDescr(tdata->tg_relation);
        int nkeys;

        // Get and validate the 'new' dictionary
        plntup = PyDict_GetItemString(pltd, "new");
        if (!plntup)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                          errmsg("TD[\"new\"] deleted, cannot modify row")));

        Py_INCREF(plntup);
        if (!PyDict_Check(plntup))
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                          errmsg("TD[\"new\"] is not a dictionary")));

        // Get dictionary keys and prepare modification arrays
        plkeys = PyDict_Keys(plntup);
        nkeys = PyList_Size(plkeys);

        modvalues = (Datum *) palloc0(tupdesc->natts * sizeof(Datum));
        modnulls = (bool *) palloc0(tupdesc->natts * sizeof(bool));
        modrepls = (bool *) palloc0(tupdesc->natts * sizeof(bool));

        // Process each key-value pair in the 'new' dictionary
        for (int i = 0; i < nkeys; i++) {
            PyObject *platt = PyList_GetItem(plkeys, i);
            char *plattstr;
            int attn;

            // Validate key is a string
            if (PyUnicode_Check(platt))
                plattstr = PLyUnicode_AsString(platt);
            else
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                              errmsg("TD[\"new\"] dictionary key at ordinal position %d is not a string", i)));

            // Find column number and validate
            attn = SPI_fnumber(tupdesc, plattstr);
            if (attn == SPI_ERROR_NOATTRIBUTE)
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                              errmsg("key \"%s\" found in TD[\"new\"] does not exist as a column in the triggering row", plattstr)));

            if (attn <= 0)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                              errmsg("cannot set system attribute \"%s\"", plattstr)));

            if (TupleDescAttr(tupdesc, attn - 1)->attgenerated)
                ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                              errmsg("cannot set generated column \"%s\"", plattstr)));

            // Get the value and convert it
            plval = PyDict_GetItem(plntup, platt);
            if (!plval)
                elog(FATAL, "Python interpreter is probably corrupted");

            Py_INCREF(plval);

            // Convert Python value to PostgreSQL Datum
            PLyObToDatum *att = &proc->result.u.tuple.atts[attn - 1];
            modvalues[attn - 1] = PLy_output_convert(att, plval, &modnulls[attn - 1]);
            modrepls[attn - 1] = true;

            Py_DECREF(plval);
            plval = NULL;
        }

        // Create the modified tuple
        rtup = heap_modify_tuple(otup, tupdesc, modvalues, modnulls, modrepls);
    }
    PG_CATCH();
    {
        // Cleanup on error
        Py_XDECREF(plntup);
        Py_XDECREF(plkeys);
        Py_XDECREF(plval);
        if (modvalues) pfree(modvalues);
        if (modnulls) pfree(modnulls);
        if (modrepls) pfree(modrepls);
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Cleanup on success
    Py_DECREF(plntup);
    Py_DECREF(plkeys);
    pfree(modvalues);
    pfree(modnulls);
    pfree(modrepls);

    error_context_stack = plerrcontext.previous;
    return rtup;
}
```