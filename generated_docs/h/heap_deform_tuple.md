# heap_deform_tuple

## Location
src/backend/access/common/heaptuple.c: 1345 - 1433

## Overview
Extracts all attribute values and null indicators from a HeapTuple into caller-provided arrays, serving as the inverse operation to heap_form_tuple.

## Definition
```c
void heap_deform_tuple(HeapTuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull)
```

## Detailed Description
This function efficiently extracts all attribute data from a HeapTuple into separate arrays. It performs several optimizations:

1. **Null handling**: Checks the tuple's null bitmap to identify null attributes and sets corresponding entries
2. **Attribute caching**: Uses and maintains `attcacheoff` values in the TupleDesc to speed up repeated access
3. **Alignment handling**: Properly handles data alignment requirements for different attribute types
4. **Variable-length attributes**: Handles varlena types with special alignment considerations
5. **Missing attributes**: For tuples with fewer attributes than expected (inheritance scenarios), fills remaining positions with missing values

The function is significantly more efficient than repeated `heap_getattr` calls, providing O(N) performance instead of O(N²) when accessing multiple attributes.

## Parameters / Member Variables
- `tuple`: The HeapTuple to extract data from
- `tupleDesc`: TupleDesc describing the expected tuple structure and types
- `values`: Caller-allocated array to receive Datum values (size = tupleDesc->natts)
- `isnull`: Caller-allocated array to receive null indicators (size = tupleDesc->natts)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHasNulls
  - HeapTupleHeaderGetNatts
  - TupleDescAttr
  - att_isnull
  - att_align_nominal
  - att_align_pointer
  - fetchatt
  - att_addlength_pointer
  - [getmissingattr](../g/getmissingattr.md)
- Called from (representative examples):
  - [heap_modify_tuple](heap_modify_tuple.md)
  - [heap_modify_tuple_by_cols](heap_modify_tuple_by_cols.md)
  - [heap_toast_insert_or_update](heap_toast_insert_or_update.md)
  - [SPI_modifytuple](../S/SPI_modifytuple.md)
  - [record_out](../r/record_out.md), record_cmp (tuple comparison functions)

## Notes and Other Information
- For pass-by-reference datatypes, returned Datum pointers point directly into the tuple data
- Handles inheritance scenarios where tuples may have more or fewer attributes than expected
- Maintains attribute offset cache (`attcacheoff`) when possible to optimize future accesses
- Sets `slow` flag when encountering null attributes or variable-length types that prevent caching
- Caller must ensure arrays are sized according to `tupleDesc->natts`
- Used extensively throughout PostgreSQL for tuple processing, type conversion, and data access
- Critical performance path in many query execution scenarios