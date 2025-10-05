# PLyGenericObject_ToComposite

## Location
[src/pl/plpython/plpy_typeio.c:1484-1557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L1484-L1557)

## Overview
Converts a Python generic object to a PostgreSQL composite type (tuple) by extracting attributes from the Python object and mapping them to the corresponding columns of the target composite type.

## Definition

```c
static Datum
PLyGenericObject_ToComposite(PLyObToDatum *arg, TupleDesc desc, PyObject *object, bool inarray)
```
## Detailed Description
This function is part of PostgreSQL's PL/Python extension and handles the conversion of Python objects to PostgreSQL composite types. It iterates through all attributes of the target PostgreSQL tuple descriptor, extracts corresponding attributes from the Python object using PyObject_GetAttrString(), and converts each attribute value to the appropriate PostgreSQL Datum using the conversion functions stored in the PLyObToDatum structure.

The function handles several important cases:
- Dropped columns are set to NULL values
- Missing attributes in the Python object result in detailed error messages with helpful hints
- Special error handling for array contexts to help users understand composite type formatting
- Proper memory management using PG_TRY/PG_CATCH blocks to ensure Python reference counts are maintained

The conversion process builds arrays of Datum values and null flags, creates a HeapTuple using heap_form_tuple(), then converts it to a Datum using heap_copy_tuple_as_datum() before cleaning up the temporary tuple.

## Parameters / Member Variables
- `*arg`: PLyObToDatum structure containing conversion metadata and attribute conversion functions
- `desc`: TupleDesc describing the target PostgreSQL composite type structure
- `*object`: Python object to be converted to the composite type
- `inarray`: Boolean flag indicating if this conversion is happening within an array context (affects error messages)
## Dependencies
- Functions called/Symbols referenced:
  - [PLyObToDatum](PLyObToDatum.md) (structure type)
  - PG_TRY/PG_CATCH/PG_RE_THROW/PG_END_TRY (exception handling macros)
  - [heap_form_tuple](../h/heap_form_tuple.md) (creates HeapTuple from values and nulls arrays)
  - [heap_copy_tuple_as_datum](../h/heap_copy_tuple_as_datum.md) (converts HeapTuple to Datum)
  - [heap_freetuple](../h/heap_freetuple.md) (frees HeapTuple memory)
  - PyObject_GetAttrString (Python C API function)
  - [palloc](../p/palloc.md)/pfree (PostgreSQL memory management)
  - ereport (PostgreSQL error reporting)

- Called from (representative examples):
  - [PLyObject_ToComposite](PLyObject_ToComposite.md) (src/pl/plpython/plpy_typeio.c:1010)

## Notes and Other Information
- This is a static function internal to the PL/Python type conversion system
- Provides detailed error messages with hints for common user mistakes, particularly around array handling
- Uses volatile variables for exception safety in PG_TRY blocks
- Includes special logic to handle the change in array interpretation behavior introduced in PostgreSQL 10
- Memory management is carefully handled to avoid leaks even in error conditions
- The function assumes the PLyObToDatum structure has been properly initialized with appropriate conversion functions for each attribute

## Simplified Source

```c
static Datum
PLyGenericObject_ToComposite(PLyObToDatum *arg, TupleDesc desc, PyObject *object, bool inarray)
{
    Datum result;
    HeapTuple tuple;
    Datum *values;
    bool *nulls;
    volatile int i;

    // Allocate arrays for tuple construction
    values = palloc(sizeof(Datum) * desc->natts);
    nulls = palloc(sizeof(bool) * desc->natts);

    // Extract each attribute from Python object by name
    for (i = 0; i < desc->natts; ++i) {
        char *key;
        PyObject *volatile value;
        PLyObToDatum *att;
        Form_pg_attribute attr = TupleDescAttr(desc, i);

        if (attr->attisdropped) {
            values[i] = (Datum) 0;
            nulls[i] = true;
            continue;
        }

        key = NameStr(attr->attname);
        value = NULL;
        att = &arg->u.tuple.atts[i];
        PG_TRY();
        {
            value = PyObject_GetAttrString(object, key);
            if (!value) {
                ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_COLUMN),
                         errmsg("attribute \"%s\" does not exist in Python object", key),
                         inarray ?
                         errhint("To return a composite type in an array, return the composite type as a Python tuple, e.g., \"[('foo',)]\".") :
                         errhint("To return null in a column, let the returned object have an attribute named after column with value None.")));
            }

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

    // Build final tuple and convert to Datum
    tuple = heap_form_tuple(desc, values, nulls);
    result = heap_copy_tuple_as_datum(tuple, desc);
    heap_freetuple(tuple);

    pfree(values);
    pfree(nulls);

    return result;
}
```