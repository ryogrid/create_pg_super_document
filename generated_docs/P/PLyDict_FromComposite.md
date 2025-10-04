# PLyDict_FromComposite

## Location
[src/pl/plpython/plpy_typeio.c:781-814](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L781-L814)

## Overview
Converts a PostgreSQL composite (row) type value from its internal storage format to a Python dictionary representation.

## Definition

```c
structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
```
## Detailed Description
This function takes a PostgreSQL composite type value stored as a Datum and converts it to a Python dictionary. It first extracts the HeapTupleHeader from the datum, retrieves the row type information (OID and type modifier), and looks up the corresponding tuple descriptor. The function ensures that input/output conversion functions are properly set up for the tuple's attributes, then constructs a temporary HeapTuple structure and delegates the actual conversion work to PLyDict_FromTuple. The tuple descriptor is properly released after use to prevent memory leaks.

## Parameters / Member Variables
- : PLyDatumToOb structure containing conversion context and type information
- : PostgreSQL Datum containing the composite type value to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [PLyDatumToOb](PLyDatumToOb.md) (type structure)
  - HeapTupleHeader (type definition)
  - [HeapTupleData](../H/HeapTupleData.md) (type structure)
  - DatumGetHeapTupleHeader (extracts tuple header from datum)
  - HeapTupleHeaderGetTypeId (gets the tuple's type OID)
  - HeapTupleHeaderGetTypMod (gets the tuple's type modifier)
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md) (looks up tuple descriptor for the row type)
  - [PLy_input_setup_tuple](PLy_input_setup_tuple.md) (sets up input conversion functions for tuple attributes)
  - [PLy_current_execution_context](PLy_current_execution_context.md) (gets current PL/Python execution context)
  - HeapTupleHeaderGetDatumLength (gets the length of tuple data)
  - [PLyDict_FromTuple](PLyDict_FromTuple.md) (performs actual tuple to dictionary conversion)
  - ReleaseTupleDesc (releases the tuple descriptor)
- Called from:
  - [PLy_input_setup_tuple](PLy_input_setup_tuple.md) (during conversion function setup)
  - [PLy_input_setup_func](PLy_input_setup_func.md) (during function setup for composite types)

## Notes and Other Information
This function serves as a bridge between PostgreSQL's internal composite type representation and Python dictionaries. It handles the complex task of extracting type metadata from the composite value's header and setting up the necessary conversion infrastructure. The function properly manages PostgreSQL's reference-counted tuple descriptors by calling ReleaseTupleDesc. The actual field-by-field conversion is delegated to PLyDict_FromTuple, which handles the details of extracting individual attribute values and converting them to appropriate Python objects.

## Simplified Source

```c
static PyObject *
PLyDict_FromComposite(PLyDatumToOb *arg, Datum d)
{
    PyObject *dict;
    HeapTupleHeader td;
    Oid tupType;
    int32 tupTypmod;
    TupleDesc tupdesc;
    HeapTupleData tmptup;

    // Extract tuple header and type info
    td = DatumGetHeapTupleHeader(d);
    tupType = HeapTupleHeaderGetTypeId(td);
    tupTypmod = HeapTupleHeaderGetTypMod(td);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

    // Set up conversion functions for tuple attributes
    PLy_input_setup_tuple(arg, tupdesc, PLy_current_execution_context()->curr_proc);

    // Build temporary HeapTuple structure
    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
    tmptup.t_data = td;

    // Convert tuple to Python dictionary
    dict = PLyDict_FromTuple(arg, &tmptup, tupdesc, true);

    ReleaseTupleDesc(tupdesc);
    return dict;
}
```