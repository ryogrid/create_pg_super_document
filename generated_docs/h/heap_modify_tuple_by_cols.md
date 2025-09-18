# heap_modify_tuple_by_cols

## Location
src/backend/access/common/heaptuple.c: 1277 - 1344

## Overview
Creates a new HeapTuple by replacing specific columns identified by column numbers rather than using a boolean replacement mask.

## Definition
```c
HeapTuple heap_modify_tuple_by_cols(HeapTuple tuple, TupleDesc tupleDesc, int nCols, const int *replCols, const Datum *replValues, const bool *replIsnull)
```

## Detailed Description
This function is a variant of `heap_modify_tuple` that provides a more convenient interface when modifying a fixed number of columns. Instead of requiring a boolean array for all columns, it accepts:

1. **Column specification**: Uses an array of 1-based column numbers (`replCols`) to identify which columns to replace
2. **Deformation and validation**: Extracts all values from the original tuple and validates column numbers
3. **Selective replacement**: Replaces only the specified columns with new values, leaving others unchanged
4. **Reformation**: Creates a new tuple using `heap_form_tuple` and preserves tuple identity metadata

The function provides better ergonomics when modifying a small, fixed set of columns compared to `heap_modify_tuple` which requires constructing a full boolean replacement array.

## Parameters / Member Variables
- `tuple`: The source HeapTuple to be modified
- `tupleDesc`: TupleDesc describing the tuple structure and types
- `nCols`: Number of columns to replace (length of the following three arrays)
- `replCols`: Array of 1-based column numbers identifying which columns to replace
- `replValues`: Array of replacement Datum values (length = nCols)
- `replIsnull`: Array of null flags for replacement values (length = nCols)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (for temporary arrays)
  - [heap_deform_tuple](heap_deform_tuple.md)
  - elog/ERROR (for column validation)
  - [heap_form_tuple](heap_form_tuple.md)
  - [pfree](../p/pfree.md) (cleanup of temporary arrays)
- Called from (representative examples):
  - [tsvector_update_trigger](../t/tsvector_update_trigger.md)
  - HeapTupleClearHeapOnly

## Notes and Other Information
- Uses 1-based column numbering (consistent with SQL standards) unlike most internal PostgreSQL functions
- Validates column numbers and raises ERROR for invalid values (≤ 0 or > numberOfAttributes)
- More efficient than `heap_modify_tuple` when only a few columns need modification
- Commonly used in trigger functions and specialized update scenarios
- Preserves tuple identity information (t_ctid, t_self, t_tableOid) from the original tuple
- Creates a completely new tuple rather than modifying in-place, similar to `heap_modify_tuple`