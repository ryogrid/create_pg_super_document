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
- : PLyObToDatum structure containing conversion metadata and attribute conversion functions
- : TupleDesc describing the target PostgreSQL composite type structure
- : Python object to be converted to the composite type
- : Boolean flag indicating if this conversion is happening within an array context (affects error messages)

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