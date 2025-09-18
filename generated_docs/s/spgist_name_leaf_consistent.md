# spgist_name_leaf_consistent

## Location
src/test/modules/spgist_name_ops/spgist_name_ops.c: 399 - 495

## Overview
Implements the leaf consistent function for SP-GiST name operator class, determining if a leaf tuple matches search predicates by reconstructing the full name value and performing comparisons.

## Definition
```c
Datum spgist_name_leaf_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function evaluates whether a leaf tuple in an SP-GiST index satisfies the search conditions. It reconstructs the complete name value by combining the reconstructed value from inner nodes with the leaf's stored data, then performs the required comparisons against all scan keys.

The function handles partial name reconstruction where:
1. Parent levels contribute a reconstructed prefix
2. Leaf tuple contains the remaining suffix
3. Combined result forms a complete name (limited to NAMEDATALEN)

All comparisons are performed using non-collation-aware string operations, and exact matches are used (no recheck required).

## Parameters / Member Variables
- `in`: Input structure (`spgLeafConsistentIn`) containing:
  - `leafDatum`: The leaf tuple's data value
  - `level`: Length of reconstructed value from parent levels
  - `reconstructedValue`: Value reconstructed from parent traversal
  - `nkeys`: Number of scan key conditions
  - `scankeys`: Array of scan key conditions to evaluate
- `out`: Output structure (`spgLeafConsistentOut`) to populate with:
  - `recheck`: Whether recheck is needed (always false)
  - `leafValue`: Complete reconstructed name value as Datum

## Dependencies
- Functions called/Symbols referenced:
  - `DatumGetTextPP`
  - [DatumGetName](../D/DatumGetName.md)
  - [palloc0](../p/palloc0.md)
  - `NAMEDATALEN`
  - `memcmp`
  - Strategy numbers (`BTLessStrategyNumber`, `BTEqualStrategyNumber`, etc.)
  - `PG_RETURN_BOOL`
- Called from (representative examples):
  - Referenced by `spgist_name_inner_consistent` function

## Notes and Other Information
- Always sets recheck to false since exact comparisons are performed
- Reconstructed names must not exceed NAMEDATALEN bytes
- Handles edge case where leaf value is empty but level > 0
- Performs byte-wise string comparison without collation awareness
- Returns boolean result indicating whether leaf tuple matches all scan conditions
- Part of test module demonstrating SP-GiST implementation for PostgreSQL name data type