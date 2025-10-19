# DatumGetRangeTypePCopy

## Location
[src/include/utils/rangetypes.h:80-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/rangetypes.h#L80-L85)

## Overview
A static inline function that converts a Datum value to a RangeType pointer, ensuring a writable copy is returned by handling detoasting and copying if necessary.

## Definition
static inline RangeType *
DatumGetRangeTypePCopy(Datum X)

## Detailed Description
DatumGetRangeTypePCopy is a conversion function that safely extracts a RangeType pointer from a Datum value, guaranteeing that the returned pointer points to a writable copy of the data. Unlike DatumGetRangeTypeP, this function uses PG_DETOAST_DATUM_COPY which ensures that if the original data is read-only (such as when it's stored in a tuple or comes from a constant), a writable copy is created.

This function is particularly important when the range type data needs to be modified after extraction, as it prevents accidental modifications to shared or read-only data structures. The function handles PostgreSQL's TOAST mechanism and ensures memory safety by providing a copy when needed.

## Parameters / Member Variables
- `X`: A Datum value that contains a RangeType object, potentially in toasted form

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_COPY (macro for detoasting and copying data)
- Called from (representative examples):
  - PG_GETARG_RANGE_P_COPY (macro for getting writable range arguments)

## Notes and Other Information
- This function is part of the fmgr functions for range type objects
- Provides a writable copy guarantee, unlike DatumGetRangeTypeP which may return read-only data
- Essential when range type data needs to be modified in-place
- Uses PostgreSQL's copy-on-demand strategy for memory efficiency
- Being defined as static inline, it provides zero-overhead abstraction while ensuring data safety
- Less frequently used than DatumGetRangeTypeP, primarily through the PG_GETARG_RANGE_P_COPY macro

## Simplified Source

```c
static inline RangeType *
DatumGetRangeTypePCopy(Datum X)
{
    // Convert Datum to writable RangeType pointer copy
    return (RangeType *) PG_DETOAST_DATUM_COPY(X);
}
```