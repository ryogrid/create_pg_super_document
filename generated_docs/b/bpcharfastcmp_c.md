# bpcharfastcmp_c

## Location
src/backend/utils/adt/varlena.c: 2049 - 2081

## Overview
A specialized fast comparison function for BpChar (blank-padded character) data types optimized for C locale sorting with proper trailing space handling.

## Definition


## Detailed Description
The `bpcharfastcmp_c` function provides optimized comparison functionality specifically for BpChar data types when using C locale collation. BpChar is PostgreSQL's CHAR(n) type that pads values with spaces to a fixed length. This function implements the BpChar semantics by using bpchartruelen() to determine the actual length of each string excluding trailing spaces, then performs a fast byte-wise comparison using memcmp(). This specialization is crucial for BpChar types because trailing spaces should not affect comparison results, requiring the true length calculation before comparison.

## Parameters / Member Variables
- `x`: First Datum containing the BpChar value to compare
- `y`: Second Datum containing the BpChar value to compare
- `ssup`: SortSupport structure (not directly used in this function but required by the interface)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetBpCharPP
  - BpChar
  - SortSupport
  - VARDATA_ANY (macro)
  - VARSIZE_ANY_EXHDR (macro)
  - bpchartruelen
  - memcmp
  - Min (macro)
  - PointerGetDatum
  - pfree
- Called from (representative examples):
  - varstr_sortsupport (when BpChar type and C locale are detected)

## Notes and Other Information
- Specifically designed for CHAR(n)/BpChar data type with trailing space semantics
- Uses bpchartruelen() to calculate the true length excluding trailing spaces before comparison
- Modeled after internal_bpchar_pattern_compare() for consistency with BpChar handling
- Provides significant performance improvement over locale-aware comparisons for C locale
- Includes proper memory management to prevent leaks from detoasted copies
- Returns standard comparison result: negative for x < y, zero for x = y, positive for x > y
- Located in src/backend/utils/adt/varlena.c at lines 2049-2081