# toast_flatten_tuple_to_datum

## Location
[src/backend/access/heap/heaptoast.c:449-562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heaptoast.c#L449-L562)

## Overview
Converts a HeapTupleHeader containing external TOAST references into a flattened Datum with all values stored inline, also decompressing any compressed fields.

## Definition
```c
Datum toast_flatten_tuple_to_datum(HeapTupleHeader tup, uint32 tup_len, TupleDesc tupleDesc)
```

## Detailed Description
The `toast_flatten_tuple_to_datum` function flattens a tuple represented as a HeapTupleHeader into a Datum suitable for use as a container type value. This is essential for PostgreSQL's rule that Datums of container types (rows, arrays, ranges, etc.) must not contain external TOAST pointers.

The function performs several transformations:
1. **Detoasts external values**: Retrieves out-of-line TOAST data using `detoast_attr`
2. **Decompresses compressed values**: Expands inline compressed fields for better tuple-level compression
3. **Preserves short-header varlenas**: Leaves inline short-header fields unchanged for efficiency

The function reconstructs the tuple as a composite Datum, properly setting datum-specific header fields like datum length, type ID, and type modifier. The result is completely self-contained with no external dependencies.

## Parameters / Member Variables
- `tup`: The HeapTupleHeader to be flattened
- `tup_len`: The total length of the input tuple
- `tupleDesc`: The tuple descriptor describing the tuple structure

## Dependencies
- Functions called/Symbols referenced:
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [detoast_attr](../d/detoast_attr.md)
  - [heap_compute_data_size](../h/heap_compute_data_size.md)
  - [heap_fill_tuple](../h/heap_fill_tuple.md)
  - HeapTupleHeaderSetNatts
  - HeapTupleHeaderSetDatumLength
  - HeapTupleHeaderSetTypeId
  - HeapTupleHeaderSetTypMod
  - VARATT_IS_EXTERNAL
  - VARATT_IS_COMPRESSED
  - MaxTupleAttributeNumber
- Called from (representative examples):
  - [heap_copy_tuple_as_datum](../h/heap_copy_tuple_as_datum.md)
  - [HeapTupleHeaderGetDatum](../H/HeapTupleHeaderGetDatum.md)

## Notes and Other Information
- Essential for enforcing the rule that container-type Datums cannot contain external TOAST pointers
- Decompresses compressed fields based on the expectation that tuple-level compression is more effective than field-level compression
- Preserves inline short-header varlena fields to avoid unnecessary work since they would be re-optimized anyway
- The result is always palloc'd in the current memory context
- Sets proper composite-Datum header fields to make the result a valid Datum
- Handles memory management carefully, cleaning up temporary allocations
- Part of the bridge between PostgreSQL's internal tuple representation and the Datum type system