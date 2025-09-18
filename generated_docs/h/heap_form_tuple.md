# heap_form_tuple

## Location
src/backend/access/common/heaptuple.c: 1116 - 1208

## Overview
Constructs a new HeapTuple from arrays of Datum values and null indicators, allocating the tuple in the current memory context.

## Definition
```c
HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, const Datum *values, const bool *isnull)
```

## Detailed Description
This is the primary function for creating HeapTuple structures from raw data. It performs the following operations:

1. **Validation**: Checks that the number of attributes doesn't exceed `MaxTupleAttributeNumber`
2. **Null detection**: Scans the `isnull` array to determine if any fields are null
3. **Size calculation**: Computes the total space needed including header, null bitmap (if needed), and data
4. **Memory allocation**: Allocates space for both the HeapTuple structure and tuple data in one chunk using `palloc0`
5. **Header initialization**: Sets up all header fields including datum length, type information, and metadata
6. **Data filling**: Delegates to `heap_fill_tuple` to populate the actual attribute data

The function ensures proper alignment and sets up the tuple to be usable both as a heap tuple and as a composite Datum if needed.

## Parameters / Member Variables
- `tupleDescriptor`: TupleDesc defining the structure and types of the tuple attributes
- `values`: Array of Datum values for each attribute (length must match tupleDescriptor->natts)
- `isnull`: Array of boolean flags indicating which attributes are null (same length as values)

## Dependencies
- Functions called/Symbols referenced:
  - MaxTupleAttributeNumber
  - ereport/ERROR (for validation)
  - offsetof/MAXALIGN (for size calculations)
  - BITMAPLEN (for null bitmap sizing)
  - [heap_compute_data_size](heap_compute_data_size.md)
  - [palloc0](../p/palloc0.md)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - HeapTupleHeaderSetDatumLength
  - HeapTupleHeaderSetTypeId
  - HeapTupleHeaderSetTypMod
  - HeapTupleHeaderSetNatts
  - [heap_fill_tuple](heap_fill_tuple.md)
- Called from (representative examples):
  - [heap_modify_tuple](heap_modify_tuple.md)
  - [heap_modify_tuple_by_cols](heap_modify_tuple_by_cols.md)
  - [SPI_modifytuple](../S/SPI_modifytuple.md)
  - [record_in](../r/record_in.md)
  - Many catalog manipulation functions

## Notes and Other Information
- The function allocates the HeapTuple structure and tuple data in a single `palloc0` call for efficiency
- Supports up to `MaxTupleAttributeNumber` attributes (typically 1664)
- Automatically includes a null bitmap in the tuple header when any attributes are null
- Sets up proper Datum headers even for tuples that may never become Datums
- Widely used throughout PostgreSQL for tuple construction in catalogs, SPI, and data manipulation
- The returned tuple has invalid item pointer (`t_self`) and table OID initially