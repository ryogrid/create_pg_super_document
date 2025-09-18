# heap_copy_tuple_as_datum

## Location
src/backend/access/common/heaptuple.c: 1080 - 1115

## Overview
Copies a HeapTuple and converts it into a composite-type Datum suitable for use in PostgreSQL's type system.

## Definition
```c
Datum heap_copy_tuple_as_datum(HeapTuple tuple, TupleDesc tupleDesc)
```

## Detailed Description
This function creates a copy of a HeapTuple and formats it as a composite-type Datum. The function handles two main scenarios:

1. **Complex case**: If the tuple contains external TOAST pointers, it delegates to `toast_flatten_tuple_to_datum` to inline those fields, ensuring the resulting Datum meets PostgreSQL's conventions for composite types.

2. **Fast path**: For simple tuples without external references, it performs a direct memory copy and sets the appropriate composite-Datum header fields (datum length, type ID, and type modifier).

The function ensures that the returned Datum has proper header information set, which may not be present if the input tuple came from disk rather than from `heap_form_tuple`.

## Parameters / Member Variables
- `tuple`: The source HeapTuple to be copied and converted
- `tupleDesc`: Tuple descriptor containing type information (type ID and type modifier) needed for the composite Datum

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHasExternal
  - toast_flatten_tuple_to_datum
  - palloc
  - memcpy
  - HeapTupleHeaderSetDatumLength
  - HeapTupleHeaderSetTypeId
  - HeapTupleHeaderSetTypMod
  - PointerGetDatum
- Called from (representative examples):
  - ExecEvalConvertRowtype
  - ExecFetchSlotHeapTupleDatum
  - SPI_returntuple
  - serialize_expr_stats

## Notes and Other Information
- The function automatically handles TOAST pointer detoasting when necessary
- Memory for the copied tuple is allocated using `palloc`
- The function sets proper composite-type Datum headers that may be missing from disk-based tuples
- Used extensively in the executor and SPI layers for type conversion operations