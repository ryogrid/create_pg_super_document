# pg_detoast_datum_slice

## Location
[src/backend/utils/fmgr/fmgr.c:1857-1863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1857-L1863)

## Overview
This function extracts a specified slice (substring) from a varlena datum, handling detoasting if necessary to efficiently retrieve only the requested portion of the data.

## Definition


## Detailed Description
pg_detoast_datum_slice is a wrapper function that provides access to a specific portion of a varlena datum without necessarily detoasting the entire value. This is particularly efficient for large toasted values where only a small portion is needed, as it can avoid the overhead of decompressing or fetching the entire datum from external storage.

The function delegates to detoast_attr_slice, which implements sophisticated optimizations for different storage formats. For externally stored compressed data, it can fetch only the compressed slices needed to reconstruct the requested portion. For uncompressed external data, it can directly fetch just the required slice from the TOAST table.

## Parameters / Member Variables
- : A pointer to the varlena structure that may be in extended (toasted) form
- : The starting offset (0-based) for the slice to extract
- : The number of bytes to extract from the starting offset

## Dependencies
- Functions called/Symbols referenced:
  - [detoast_attr_slice](../d/detoast_attr_slice.md) (function that performs the actual slice extraction)
  - [varlena](../v/varlena.md) (structure type)
- Called from (representative examples):
  - PG_DETOAST_DATUM_SLICE (macro)
  - Functions implementing substring operations on text and bytea types

## Notes and Other Information
This function is particularly useful for implementing substring operations on large text or bytea values that may be stored in TOAST tables. By avoiding the need to detoast the entire value when only a slice is needed, it can provide significant performance improvements for operations on large variable-length data. The function is commonly used through the PG_DETOAST_DATUM_SLICE macro in PostgreSQL's function interface system. The slice extraction handles various edge cases such as offsets beyond the data length and automatically adjusts slice lengths that would exceed the available data.