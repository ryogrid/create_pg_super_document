# PLyMapping_ToComposite

## Location
[src/pl/plpython/plpy_typeio.c:1342-1406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L1342-L1406)

## Overview
Converts a Python mapping object (dictionary) to a PostgreSQL composite type by extracting values for each column based on matching keys.

## Definition

```c
static Datum
PLyMapping_ToComposite(PLyObToDatum *arg, TupleDesc desc, PyObject *mapping)
```
## Detailed Description
This function constructs a PostgreSQL composite type from a Python mapping object by iterating through the tuple descriptor's attributes and extracting corresponding values from the mapping using column names as keys. It creates heap tuple structures and handles proper memory management with exception safety. The function validates that all required columns are present in the mapping and provides helpful error messages for missing keys.

The conversion process involves:
1. Allocation of arrays for datum values and null flags
2. Iteration through all tuple descriptor attributes
3. Extraction of values from the Python mapping using attribute names as keys
4. Individual conversion of each extracted value using appropriate converters
5. Construction of HeapTuple and final Datum representation
6. Proper cleanup of temporary structures and memory

## Parameters / Member Variables
- `*arg`: PLyObToDatum structure containing composite type conversion context and attribute converters
- `desc`: TupleDesc describing the structure and types of the target composite type
- `*mapping`: Python mapping object (typically a dictionary) containing the source data
## Dependencies
- Functions called/Symbols referenced:
  - PyMapping_Check (Python API validation)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - TupleDescAttr (tuple descriptor attribute access)
  - PyMapping_GetItemString (Python mapping value extraction)
  - [heap_form_tuple](../h/heap_form_tuple.md) (creates HeapTuple from arrays)
  - [heap_copy_tuple_as_datum](../h/heap_copy_tuple_as_datum.md) (converts HeapTuple to Datum)
  - [heap_freetuple](../h/heap_freetuple.md) (frees HeapTuple memory)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - PG_TRY/PG_CATCH/PG_RE_THROW/PG_END_TRY (exception handling)
  - Py_XDECREF (Python reference counting)
- Called from (representative examples):
  - [PLyObject_ToComposite](PLyObject_ToComposite.md) (src/pl/plpython/plpy_typeio.c:1007)

## Notes and Other Information
- Validates input with PyMapping_Check assertion to ensure proper type handling
- Handles dropped columns by setting them to null automatically
- Uses column names from the tuple descriptor as dictionary keys for value lookup
- Provides user-friendly error messages when required keys are missing from the mapping
- Includes helpful hints suggesting the use of None values for null columns
- Uses PostgreSQL's exception handling system to ensure proper Python reference cleanup
- The volatile qualifier on the loop variable ensures proper behavior across exception boundaries
- Memory allocation and cleanup is handled carefully to prevent leaks in error conditions

## Simplified Source

```c
static Datum
PLyMapping_ToComposite(PLyObToDatum *arg, TupleDesc desc, PyObject *mapping)
{
    Assert(PyMapping_Check(mapping));

    // Allocate arrays for datum values and null flags
    Datum *values = palloc(sizeof(Datum) * desc->natts);
    bool *nulls = palloc(sizeof(bool) * desc->natts);

    // Process each attribute in the tuple descriptor
    for (volatile int i = 0; i < desc->natts; ++i) {
        Form_pg_attribute attr = TupleDescAttr(desc, i);

        // Handle dropped columns
        if (attr->attisdropped) {
            values[i] = (Datum) 0;
            nulls[i] = true;
            continue;
        }

        // Extract value from mapping using column name as key
        char *key = NameStr(attr->attname);
        PyObject *volatile value = NULL;
        PLyObToDatum *att = &arg->u.tuple.atts[i];

        PG_TRY();
        {
            value = PyMapping_GetItemString(mapping, key);
            if (!value)
                ereport(ERROR, "key \"%s\" not found in mapping", key);

            // Convert Python value to PostgreSQL datum
            values[i] = att->func(att, value, &nulls[i], false);

            Py_XDECREF(value);
            value = NULL;
        }
        PG_CATCH();
        {
            Py_XDECREF(value);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }

    // Build HeapTuple and convert to Datum
    HeapTuple tuple = heap_form_tuple(desc, values, nulls);
    Datum result = heap_copy_tuple_as_datum(tuple, desc);

    // Clean up memory
    heap_freetuple(tuple);
    pfree(values);
    pfree(nulls);

    return result;
}
```