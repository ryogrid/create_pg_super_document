# varstrfastcmp_c

## Location
[src/backend/utils/adt/varlena.c:2012-2048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2012-L2048)

## Overview
A fast comparison function for variable-length string types optimized for C locale sorting using memcmp() for maximum performance.

## Definition

```c
static int
varstrfastcmp_c(Datum x, Datum y, SortSupport ssup)
```
## Detailed Description
The `varstrfastcmp_c` function provides an optimized comparison implementation for variable-length string data types when using C locale collation. It bypasses expensive locale-aware string comparison functions by using the highly efficient memcmp() system call to compare the raw bytes of two strings. The function handles variable-length strings by extracting the actual data portion and comparing only up to the length of the shorter string, then using length as a tiebreaker if the common prefix is identical. This approach provides significant performance improvements over strcoll()-based comparisons when locale-specific ordering is not required.

## Parameters / Member Variables
- `x`: First Datum containing the VarString to compare
- `y`: Second Datum containing the VarString to compare  
- `ssup`: SortSupport structure (not directly used in this function but required by the interface)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetVarStringPP
  - [VarString](../V/VarString.md)
  - [SortSupport](../S/SortSupport.md)
  - VARDATA_ANY (macro)
  - VARSIZE_ANY_EXHDR (macro)
  - memcmp
  - Min (macro)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [varstr_sortsupport](varstr_sortsupport.md) (when C locale is detected)

## Notes and Other Information
- Only used when LC_COLLATE = C, providing byte-wise comparison semantics
- Includes careful memory management to avoid leaks by freeing detoasted copies
- Uses memcmp() for maximum performance, comparing raw byte values rather than character semantics
- Returns standard comparison result: negative for x < y, zero for x = y, positive for x > y
- Handles different string lengths by using the shorter length for comparison, then length difference as tiebreaker
- Located in src/backend/utils/adt/varlena.c at lines 2012-2048