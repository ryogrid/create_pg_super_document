# btfloat8fastcmp

## Location
src/backend/utils/adt/float.c: 976 - 984

## Overview
Internal static function that provides optimized three-way comparison between two double-precision floating-point numbers for sort support operations in PostgreSQL.

## Definition


## Detailed Description
This function is an optimized comparison function specifically designed for use with PostgreSQL's SortSupport framework. Unlike btfloat8cmp which uses the standard PostgreSQL function call interface, btfloat8fastcmp operates directly on Datum values, avoiding the overhead of the fmgr (function manager) interface. This makes it more efficient for sorting operations where many comparisons are needed. The function extracts double-precision floating-point values from the input Datums and delegates the actual comparison to float8_cmp_internal.

## Parameters / Member Variables
- : First Datum containing a double-precision floating-point value
- : Second Datum containing a double-precision floating-point value  
- : SortSupport structure (unused in this function but required by the interface)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetFloat8 (macro for extracting float8 from Datum)
  - float8_cmp_internal (performs the actual comparison)
  - SortSupport (type definition for sort support structure)

- Called from (representative examples):
  - btfloat8sortsupport (assigns this function as a comparison routine)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:976-984
- This is a static function, meaning it's only accessible within the same source file
- Used as a performance optimization for sorting operations involving double-precision values
- The SortSupport parameter is part of the required interface but is not used by this specific function
- Returns an int value following the standard comparison convention (-1, 0, 1)
- Part of PostgreSQL's sort support infrastructure for efficient sorting and indexing