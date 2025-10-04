# spg_text_leaf_consistent

## Location
[src/backend/access/spgist/spgtextproc.c:574-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgtextproc.c#L574-L701)

## Overview
The leaf consistent function for SP-GiST text operator class that tests search conditions against actual stored text values at leaf nodes to determine if they match the query.

## Definition
```c
Datum spg_text_leaf_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is invoked during SP-GiST index scans when reaching leaf nodes. It reconstructs the complete text value by combining the reconstructed value from inner nodes with the leaf tuple's stored suffix. The function then performs exact comparisons between this reconstructed full text value and the search query according to the specified strategy (equality, less than, greater than, prefix matching). For prefix queries, it can optimize by checking if the reconstructed inner node value already satisfies the prefix condition. The function handles both collation-aware and non-collation-aware comparisons and returns a boolean result indicating whether the leaf tuple matches the search conditions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `in` (spgLeafConsistentIn *): Input structure with leaf tuple data, scan keys, and reconstruction context
  - `out` (spgLeafConsistentOut *): Output structure to be filled with match result and reconstructed leaf value

## Dependencies
- Functions called/Symbols referenced:
  - [spgLeafConsistentIn](spgLeafConsistentIn.md), spgLeafConsistentOut (SP-GiST framework structures)
  - DatumGetTextPP (text datum conversion)
  - SET_VARSIZE, VARDATA (text/varlena manipulation macros)
  - [text_starts_with](../t/text_starts_with.md), DirectFunctionCall2Coll (prefix comparison functions)
  - PG_GET_COLLATION (collation context)
  - SPG_IS_COLLATION_AWARE_STRATEGY (strategy testing macro)
  - [pg_verifymbstr](../p/pg_verifymbstr.md) (multibyte string validation)
  - [varstr_cmp](../v/varstr_cmp.md) (collation-aware string comparison)
  - BTLessStrategyNumber, BTEqualStrategyNumber, etc. (comparison strategy constants)
- Called from (representative examples):
  - SP-GiST framework during leaf node evaluation (no direct references found)

## Notes and Other Information
- Sets recheck to false since all tests are exact and no post-filtering is needed
- Handles the special case where leaf value is empty but level > 0 by reusing the reconstructed value
- For prefix queries, can short-circuit when the reconstructed inner value already contains the full query prefix
- Supports both collation-aware and byte-wise string comparisons depending on the strategy
- Validates multibyte string encoding in debug builds for collation-aware comparisons
- Critical for final result accuracy as it performs the definitive match test against actual stored values

## Simplified Source

```c
Datum spg_text_leaf_consistent(PG_FUNCTION_ARGS) {
    spgLeafConsistentIn *in = (spgLeafConsistentIn *) PG_GETARG_POINTER(0);
    spgLeafConsistentOut *out = (spgLeafConsistentOut *) PG_GETARG_POINTER(1);
    int level = in->level;
    text *leafValue = DatumGetTextPP(in->leafDatum);
    text *reconstrValue = NULL;
    char *fullValue;
    int fullLen;
    bool res = true;

    // All tests are exact
    out->recheck = false;

    // Get reconstructed value from parent
    if (DatumGetPointer(in->reconstructedValue))
        reconstrValue = (text *) DatumGetPointer(in->reconstructedValue);

    // Reconstruct full string: parent reconstruction + leaf data
    fullLen = level + VARSIZE_ANY_EXHDR(leafValue);
    if (VARSIZE_ANY_EXHDR(leafValue) == 0 && level > 0) {
        // Empty leaf, use reconstructed value as-is
        fullValue = VARDATA(reconstrValue);
        out->leafValue = PointerGetDatum(reconstrValue);
    } else {
        // Combine reconstructed value with leaf data
        text *fullText = palloc(VARHDRSZ + fullLen);
        SET_VARSIZE(fullText, VARHDRSZ + fullLen);
        fullValue = VARDATA(fullText);

        if (level)
            memcpy(fullValue, VARDATA(reconstrValue), level);
        if (VARSIZE_ANY_EXHDR(leafValue) > 0)
            memcpy(fullValue + level, VARDATA_ANY(leafValue),
                   VARSIZE_ANY_EXHDR(leafValue));
        out->leafValue = PointerGetDatum(fullText);
    }

    // Test against all scan keys
    for (int j = 0; j < in->nkeys; j++) {
        StrategyNumber strategy = in->scankeys[j].sk_strategy;
        text *query = DatumGetTextPP(in->scankeys[j].sk_argument);
        int queryLen = VARSIZE_ANY_EXHDR(query);
        int r;

        // Special handling for prefix strategy
        if (strategy == RTPrefixStrategyNumber) {
            // Optimization: if level >= queryLen, prefix already matches
            res = (level >= queryLen) ||
                  DatumGetBool(DirectFunctionCall2Coll(text_starts_with,
                                                       PG_GET_COLLATION(),
                                                       out->leafValue,
                                                       PointerGetDatum(query)));
            if (!res) break;
            continue;
        }

        // Perform comparison
        if (SPG_IS_COLLATION_AWARE_STRATEGY(strategy)) {
            // Collation-aware comparison
            strategy -= SPG_STRATEGY_ADDITION;
            r = varstr_cmp(fullValue, fullLen,
                          VARDATA_ANY(query), queryLen,
                          PG_GET_COLLATION());
        } else {
            // Byte-wise comparison
            r = memcmp(fullValue, VARDATA_ANY(query), Min(queryLen, fullLen));
            if (r == 0) {
                if (queryLen > fullLen)
                    r = -1;
                else if (queryLen < fullLen)
                    r = 1;
            }
        }

        // Apply strategy
        switch (strategy) {
            case BTLessStrategyNumber:
                res = (r < 0);
                break;
            case BTLessEqualStrategyNumber:
                res = (r <= 0);
                break;
            case BTEqualStrategyNumber:
                res = (r == 0);
                break;
            case BTGreaterEqualStrategyNumber:
                res = (r >= 0);
                break;
            case BTGreaterStrategyNumber:
                res = (r > 0);
                break;
            default:
                elog(ERROR, "unrecognized strategy number: %d", strategy);
                res = false;
        }

        if (!res) break;
    }

    PG_RETURN_BOOL(res);
}
```