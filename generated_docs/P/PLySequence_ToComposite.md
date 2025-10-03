# PLySequence_ToComposite

## Location
[src/pl/plpython/plpy_typeio.c:1407-1483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L1407-L1483)

## Overview
Converts a Python sequence (list, tuple) to a PostgreSQL composite type by mapping sequence elements to tuple attributes by positional order.

## Definition

```c
static Datum
PLySequence_ToComposite(PLyObToDatum *arg, TupleDesc desc, PyObject *sequence)
```
## Detailed Description
This function constructs a PostgreSQL composite type from a Python sequence by mapping elements positionally to the tuple descriptor's attributes. It enforces strict length matching between the sequence and the number of non-dropped columns to prevent developer errors. The function creates heap tuple structures with proper exception safety and memory management.

The conversion process involves:
1. Strict validation that sequence length matches the number of non-dropped columns
2. Allocation of arrays for datum values and null flags
3. Positional iteration through sequence elements and tuple attributes
4. Individual conversion of each element using appropriate type converters
5. Construction of HeapTuple and final Datum representation
6. Proper cleanup of temporary structures and allocated memory

## Parameters / Member Variables
- `*arg`: PLyObToDatum structure containing composite type conversion context and attribute converters
- `desc`: TupleDesc describing the structure and types of the target composite type
- `*sequence`: Python sequence object (list, tuple, etc.) containing the source data in positional order
## Dependencies
- Functions called/Symbols referenced:
  - PySequence_Check (Python API validation)
  - PySequence_Length (Python API length retrieval)
  - PySequence_GetItem (Python API element access)
  - TupleDescAttr (tuple descriptor attribute access)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [heap_form_tuple](../h/heap_form_tuple.md) (creates HeapTuple from arrays)
  - [heap_copy_tuple_as_datum](../h/heap_copy_tuple_as_datum.md) (converts HeapTuple to Datum)
  - [heap_freetuple](../h/heap_freetuple.md) (frees HeapTuple memory)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - PG_TRY/PG_CATCH/PG_RE_THROW/PG_END_TRY (exception handling)
  - Py_XDECREF (Python reference counting)
- Called from (representative examples):
  - [PLyObject_ToComposite](PLyObject_ToComposite.md) (src/pl/plpython/plpy_typeio.c:1004)

## Notes and Other Information
- Validates input with PySequence_Check assertion to ensure proper type handling
- Enforces strict length matching - sequence must contain exactly the same number of elements as non-dropped columns
- Automatically handles dropped columns by setting them to null and skipping them in sequence indexing
- Uses two separate index variables: i for tuple attributes and idx for sequence positions
- Provides clear error messages when sequence length doesn't match expected column count
- Uses PostgreSQL's exception handling system to ensure proper Python reference cleanup
- The volatile qualifiers on loop variables ensure proper behavior across exception boundaries
- Memory allocation and cleanup is handled carefully to prevent leaks in error conditions
- More restrictive than PLyMapping_ToComposite as it requires exact positional correspondence