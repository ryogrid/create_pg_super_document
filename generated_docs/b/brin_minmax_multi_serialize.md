# brin_minmax_multi_serialize

## Location
src/backend/access/brin/brin_minmax_multi.c: 2380 - 2398

## Overview
Serializes BRIN minmax multi-column range data structures for storage, converting in-memory range representations to their persistent binary format.

## Definition
```c
static void brin_minmax_multi_serialize(BrinDesc *bdesc, Datum src, Datum *dst)
```

## Detailed Description
This static function is responsible for converting in-memory BRIN minmax multi-column range data structures into their serialized form for persistent storage. The function operates in two main phases:

1. **Compaction Phase**: Uses `compactify_ranges` to compress the accumulated range values down to the target maximum number of values specified in the range structure. This is particularly important in batch processing mode where many values may have been accumulated.

2. **Serialization Phase**: Converts the compacted and sorted ranges into a `SerializedRanges` structure suitable for storage using `brin_range_serialize`.

The function includes an assertion to ensure that all ranges are properly sorted before serialization, which is critical for the correct functioning of BRIN index operations. The serialized output is stored in the destination datum array.

## Parameters / Member Variables
- `bdesc`: BRIN descriptor containing index metadata and configuration
- `src`: Source datum containing the in-memory `Ranges` structure to be serialized
- `dst`: Destination datum array where the serialized `SerializedRanges` structure is stored

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetPointer: Extracts pointer from Datum
  - compactify_ranges: Compresses ranges to target maximum values
  - brin_range_serialize: Converts ranges to serialized format
  - PointerGetDatum: Converts pointer to Datum
- Called from (representative examples):
  - brin_minmax_multi_add_value: Called during batch processing operations

## Notes and Other Information
- Function is declared as static, indicating it's only used within the brin_minmax_multi.c file
- The assertion ensures data integrity by verifying that all values are sorted before serialization
- Critical for batch mode operations where multiple values are accumulated before being written to storage
- The compaction step optimizes storage by reducing the number of stored ranges to the configured maximum
- Part of the BRIN minmax multi-column infrastructure for efficient range-based indexing