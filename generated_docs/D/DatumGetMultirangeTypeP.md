# DatumGetMultirangeTypeP

## Location
[src/include/utils/multirangetypes.h:48-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/multirangetypes.h#L48-L53)

## Overview
DatumGetMultirangeTypeP is an inline function that converts a PostgreSQL Datum value to a MultirangeType pointer, handling potential TOAST decompression automatically.

## Definition


## Detailed Description
This function serves as a type conversion utility for PostgreSQL's multirange data types. It takes a Datum (PostgreSQL's universal data type container) and safely converts it to a MultirangeType pointer. The function automatically handles TOAST (The Oversized-Attribute Storage Technique) decompression if the data was stored in compressed or out-of-line format. This is essential for working with potentially large multirange values that may have been stored using PostgreSQL's TOAST mechanism.

The function is implemented as a static inline function in the header file, making it efficiently accessible throughout the codebase without function call overhead.

## Parameters / Member Variables
- : A Datum value containing a multirange type that needs to be converted to a MultirangeType pointer

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM (macro for TOAST decompression)
  - MultirangeType (return type)
- Called from (representative examples):
  - [multirangesel](../m/multirangesel.md) (multirange selectivity estimation)
  - [range_gist_consistent](../r/range_gist_consistent.md) (GiST index consistency checking)
  - [multirange_gist_compress](../m/multirange_gist_compress.md) (GiST index compression)
  - [multirange_gist_consistent](../m/multirange_gist_consistent.md) (multirange GiST consistency)
  - [compute_range_stats](../c/compute_range_stats.md) (range statistics computation)
  - PG_GETARG_MULTIRANGE_P (macro for function argument retrieval)

## Notes and Other Information
- This is a foundational utility function used extensively in multirange type operations
- The inline implementation ensures optimal performance for frequent conversions
- Automatically handles TOAST decompression, making it safe to use with potentially compressed multirange data
- Part of PostgreSQL's type system infrastructure for multirange types introduced in PostgreSQL 14