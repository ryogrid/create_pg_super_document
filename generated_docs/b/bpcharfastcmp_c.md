# bpcharfastcmp_c

## Location
[src/backend/utils/adt/varlena.c:2049-2081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2049-L2081)

## Overview
A specialized fast comparison function for BpChar (blank-padded character) data types optimized for C locale sorting with proper trailing space handling.

## Definition

```c
static int
bpcharfastcmp_c(Datum x, Datum y, SortSupport ssup)
```
## Detailed Description
The `bpcharfastcmp_c` function provides optimized comparison functionality specifically for BpChar data types when using C locale collation. BpChar is PostgreSQL's CHAR(n) type that pads values with spaces to a fixed length. This function implements the BpChar semantics by using bpchartruelen() to determine the actual length of each string excluding trailing spaces, then performs a fast byte-wise comparison using memcmp(). This specialization is crucial for BpChar types because trailing spaces should not affect comparison results, requiring the true length calculation before comparison.

## Parameters / Member Variables
- `x`: First Datum containing the BpChar value to compare
- `y`: Second Datum containing the BpChar value to compare
- `ssup`: SortSupport structure (not directly used in this function but required by the interface)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetBpCharPP
  - [BpChar](../B/BpChar.md)
  - [SortSupport](../S/SortSupport.md)
  - VARDATA_ANY (macro)
  - VARSIZE_ANY_EXHDR (macro)
  - [bpchartruelen](bpchartruelen.md)
  - memcmp
  - Min (macro)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [varstr_sortsupport](../v/varstr_sortsupport.md) (when BpChar type and C locale are detected)

## Notes and Other Information
- Specifically designed for CHAR(n)/BpChar data type with trailing space semantics
- Uses bpchartruelen() to calculate the true length excluding trailing spaces before comparison
- Modeled after internal_bpchar_pattern_compare() for consistency with BpChar handling
- Provides significant performance improvement over locale-aware comparisons for C locale
- Includes proper memory management to prevent leaks from detoasted copies
- Returns standard comparison result: negative for x < y, zero for x = y, positive for x > y
- Located in src/backend/utils/adt/varlena.c at lines 2049-2081

## Simplified Source

```c
static int bpcharfastcmp_c(Datum x, Datum y, SortSupport ssup) {
    // Extract BpChar values from datums
    BpChar *arg1 = DatumGetBpCharPP(x);
    BpChar *arg2 = DatumGetBpCharPP(y);

    // Get pointers to string data
    char *a1p = VARDATA_ANY(arg1);
    char *a2p = VARDATA_ANY(arg2);

    // Calculate true lengths excluding trailing spaces (BpChar semantics)
    int len1 = bpchartruelen(a1p, VARSIZE_ANY_EXHDR(arg1));
    int len2 = bpchartruelen(a2p, VARSIZE_ANY_EXHDR(arg2));

    // Fast byte-wise comparison using memcmp
    int result = memcmp(a1p, a2p, Min(len1, len2));

    // If common prefix is equal, compare by true length
    if (result == 0 && len1 != len2)
        result = (len1 < len2) ? -1 : 1;

    // Clean up any detoasted copies to prevent memory leaks
    if (PointerGetDatum(arg1) != x) pfree(arg1);
    if (PointerGetDatum(arg2) != y) pfree(arg2);

    return result;
}
```