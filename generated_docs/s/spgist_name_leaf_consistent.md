# spgist_name_leaf_consistent

## Location
[src/test/modules/spgist_name_ops/spgist_name_ops.c:399-495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/spgist_name_ops/spgist_name_ops.c#L399-L495)

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

## Simplified Source

```c
Datum spgist_name_leaf_consistent(PG_FUNCTION_ARGS) {
    spgLeafConsistentIn *in = (spgLeafConsistentIn *) PG_GETARG_POINTER(0);
    spgLeafConsistentOut *out = (spgLeafConsistentOut *) PG_GETARG_POINTER(1);

    // No recheck needed - exact comparisons
    out->recheck = false;

    text *leafValue = DatumGetTextPP(in->leafDatum);
    text *reconstrValue = NULL;
    if (DatumGetPointer(in->reconstructedValue))
        reconstrValue = (text *) DatumGetPointer(in->reconstructedValue);

    // Reconstruct complete name from parent prefix + leaf suffix
    char *fullValue = palloc0(NAMEDATALEN);
    int level = in->level;
    int fullLen = level + VARSIZE_ANY_EXHDR(leafValue);

    if (VARSIZE_ANY_EXHDR(leafValue) == 0 && level > 0) {
        // Special case: empty leaf value, use only reconstructed part
        memcpy(fullValue, VARDATA(reconstrValue), VARSIZE_ANY_EXHDR(reconstrValue));
    } else {
        // Copy reconstructed prefix and leaf suffix
        if (level)
            memcpy(fullValue, VARDATA(reconstrValue), level);
        if (VARSIZE_ANY_EXHDR(leafValue) > 0)
            memcpy(fullValue + level, VARDATA_ANY(leafValue), VARSIZE_ANY_EXHDR(leafValue));
    }

    out->leafValue = PointerGetDatum(fullValue);

    // Test against all scan key conditions
    bool result = true;
    for (int j = 0; j < in->nkeys; j++) {
        StrategyNumber strategy = in->scankeys[j].sk_strategy;
        Name queryName = DatumGetName(in->scankeys[j].sk_argument);
        char *queryStr = NameStr(*queryName);
        int queryLen = strlen(queryStr);

        // Compare full reconstructed value with query string
        int comparison = memcmp(fullValue, queryStr, Min(queryLen, fullLen));

        // Handle case where prefix matches but lengths differ
        if (comparison == 0) {
            if (queryLen > fullLen)
                comparison = -1;
            else if (queryLen < fullLen)
                comparison = 1;
        }

        // Apply strategy-specific comparison
        switch (strategy) {
            case BTLessStrategyNumber:
                result = (comparison < 0);
                break;
            case BTLessEqualStrategyNumber:
                result = (comparison <= 0);
                break;
            case BTEqualStrategyNumber:
                result = (comparison == 0);
                break;
            case BTGreaterEqualStrategyNumber:
                result = (comparison >= 0);
                break;
            case BTGreaterStrategyNumber:
                result = (comparison > 0);
                break;
        }

        if (!result)
            break; // Short circuit on first failure
    }

    PG_RETURN_BOOL(result);
}
```