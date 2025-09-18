# brin_bloom_union

## Location
[src/backend/access/brin/brin_bloom.c:666-716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_bloom.c#L666-L716)

## Overview
Combines two BRIN bloom filter summaries by performing a bitwise union operation, updating the first bloom filter to represent the union of both filters.

## Definition
```c
Datum brin_bloom_union(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a BRIN operator function that implements the union operation for bloom filter summaries. It is used during BRIN index maintenance operations like page splits or range merging where two bloom filters need to be combined:

1. **Parameter validation**: Ensures both BrinValues have the same attribute number and neither is all-null
2. **Filter extraction**: Retrieves both bloom filters from their respective BrinValues, handling TOAST decompression
3. **Compatibility check**: Verifies that both filters have the same parameters (number of bits and hash functions)
4. **Bitwise union**: Performs a bitwise OR operation on the filter data to combine them
5. **Statistics update**: Recalculates the number of set bits using pg_popcount
6. **Memory management**: Handles decompressed filter cleanup and updates the BrinValues structure

The operation is destructive to the first filter (col_a) but leaves the second filter (col_b) unchanged.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function argument structure containing:
  - `col_a`: First BrinValues structure (will be modified to contain the union)
  - `col_b`: Second BrinValues structure (remains unchanged)

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM
  - [pg_popcount](../p/pg_popcount.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [pfree](../p/pfree.md)
  - PG_RETURN_VOID
- Called from (representative examples):
  - This is a BRIN operator function called by the BRIN index maintenance infrastructure

## Notes and Other Information
- This is a PostgreSQL internal function following the PG_FUNCTION_ARGS convention for operator functions
- The function includes several assertions to ensure filter compatibility and data integrity
- Currently assumes both bloom filters have identical parameters (nbits, nhashes) - a future improvement mentioned in comments would add a 'can union' function to verify compatibility
- The function handles TOAST decompression transparently and manages memory allocation/deallocation
- Uses bitwise OR operation which is the mathematical basis for bloom filter union operations
- Updates the nbits_set field to maintain accurate statistics about filter density
- The function is designed to be called during BRIN index operations that require combining summaries from different page ranges
- Memory management is carefully handled to avoid leaks when working with potentially compressed (TOAST) data