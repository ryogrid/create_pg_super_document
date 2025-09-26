# DatumGetRangeTypeP

## Location
src/include/utils/rangetypes.h: 74 - 79

## Overview
A static inline function that converts a Datum value to a RangeType pointer, handling detoasting if necessary.

## Definition

```c
static inline RangeType *
DatumGetRangeTypeP(Datum X)
```
## Detailed Description
DatumGetRangeTypeP is a conversion function that safely extracts a RangeType pointer from a Datum value. It uses PostgreSQL's detoasting mechanism (PG_DETOAST_DATUM) to handle cases where the range type data might be stored in compressed or out-of-line form (TOAST). This function is essential for accessing range type data that has been passed through PostgreSQL's function manager (fmgr) interface.

The function is defined as a static inline function in the header file, meaning it's expanded at compile time for optimal performance when converting Datum values to RangeType pointers throughout the range type subsystem.

## Parameters / Member Variables
- : A Datum value that contains a RangeType object, potentially in toasted form

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM (macro for detoasting data)
- Called from (representative examples):
  - multirange_in (multirange input function)
  - range_gist_consistent (GiST index consistency checking)
  - spg_range_quad_choose (SP-GiST index operations)
  - rangesel (range selectivity estimation)
  - PG_GETARG_RANGE_P (macro for getting range arguments)

## Notes and Other Information
- This function is part of the fmgr functions for range type objects
- It's widely used across the range type implementation, appearing in over 40 different functions
- The function handles PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) mechanism automatically
- Being defined as static inline, it provides zero-overhead abstraction for Datum-to-RangeType conversion
- Essential for all range type operations that receive range data through the function manager interface