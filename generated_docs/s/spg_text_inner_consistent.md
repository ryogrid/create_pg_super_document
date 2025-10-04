# spg_text_inner_consistent

## Location
[src/backend/access/spgist/spgtextproc.c:426-573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgtextproc.c#L426-L573)

## Overview
The inner consistent function for SP-GiST text operator class that determines which child nodes to visit during index traversal by testing search conditions against reconstructed key values.

## Definition
```c
Datum spg_text_inner_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is invoked during SP-GiST index scans to determine which child nodes should be traversed. It reconstructs the full key value at each inner node by combining the parent's reconstructed value, any prefix stored at the current node, and the node labels (characters). For each child node, it tests all scan key conditions against the reconstructed value to determine if that subtree could contain matching tuples. The function handles various comparison strategies (equality, less than, greater than, prefix matching) and considers collation rules when appropriate. It returns a list of child nodes that should be visited along with their reconstructed values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `in` (spgInnerConsistentIn *): Input structure with scan keys, node information, and reconstruction context
  - `out` (spgInnerConsistentOut *): Output structure to be filled with qualifying child nodes

## Dependencies
- Functions called/Symbols referenced:
  - [spgInnerConsistentIn](spgInnerConsistentIn.md), spgInnerConsistentOut (SP-GiST framework structures)
  - [lc_collate_is_c](../l/lc_collate_is_c.md), PG_GET_COLLATION (collation handling)
  - DatumGetTextPP, DatumGetInt16 (datum conversion functions)
  - SET_VARSIZE, VARDATA (text/varlena manipulation macros)
  - SPG_IS_COLLATION_AWARE_STRATEGY (strategy testing macro)
  - BTLessStrategyNumber, BTEqualStrategyNumber, etc. (comparison strategy constants)
  - [datumCopy](../d/datumCopy.md) (creates deep copy of datum)
- Called from (representative examples):
  - SP-GiST framework during index scans (no direct references found)

## Notes and Other Information
- Handles both collation-aware and non-collation-aware text comparisons
- For non-C collations, may need to traverse the entire subtree due to complex sorting rules
- Reconstructs values incrementally, building upon parent node's reconstructed value
- Supports dummy node labels (values ≤ 0) that don't contribute character data
- Critical for query performance as it determines search tree pruning effectiveness
- The reconstructed value may end with partial multibyte characters, requiring careful handling of encoding-sensitive operations

## Simplified Source

```c
Datum spg_text_inner_consistent(PG_FUNCTION_ARGS) {
    spgInnerConsistentIn *in = (spgInnerConsistentIn *) PG_GETARG_POINTER(0);
    spgInnerConsistentOut *out = (spgInnerConsistentOut *) PG_GETARG_POINTER(1);
    bool collate_is_c = lc_collate_is_c(PG_GET_COLLATION());
    text *reconstructedValue = (text *) DatumGetPointer(in->reconstructedValue);
    text *prefixText = NULL;
    int prefixSize = 0;
    int maxReconstrLen = in->level + 1;

    // Calculate reconstruction length including prefix
    if (in->hasPrefix) {
        prefixText = DatumGetTextPP(in->prefixDatum);
        prefixSize = VARSIZE_ANY_EXHDR(prefixText);
        maxReconstrLen += prefixSize;
    }

    // Build reconstruction template
    text *reconstrText = palloc(VARHDRSZ + maxReconstrLen);
    SET_VARSIZE(reconstrText, VARHDRSZ + maxReconstrLen);

    // Copy parent reconstruction and prefix
    if (in->level)
        memcpy(VARDATA(reconstrText), VARDATA(reconstructedValue), in->level);
    if (prefixSize)
        memcpy(((char *) VARDATA(reconstrText)) + in->level,
               VARDATA_ANY(prefixText), prefixSize);

    // Prepare output arrays
    out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
    out->levelAdds = (int *) palloc(sizeof(int) * in->nNodes);
    out->reconstructedValues = (Datum *) palloc(sizeof(Datum) * in->nNodes);
    out->nNodes = 0;

    // Test each child node
    for (int i = 0; i < in->nNodes; i++) {
        int16 nodeChar = DatumGetInt16(in->nodeLabels[i]);
        int thisLen;
        bool res = true;

        // Complete reconstruction with node character
        if (nodeChar <= 0) {
            thisLen = maxReconstrLen - 1; // Dummy node
        } else {
            ((unsigned char *) VARDATA(reconstrText))[maxReconstrLen - 1] = nodeChar;
            thisLen = maxReconstrLen;
        }

        // Test against all scan keys
        for (int j = 0; j < in->nkeys; j++) {
            StrategyNumber strategy = in->scankeys[j].sk_strategy;
            text *inText = DatumGetTextPP(in->scankeys[j].sk_argument);
            int inSize = VARSIZE_ANY_EXHDR(inText);

            // Handle collation-aware strategies
            if (SPG_IS_COLLATION_AWARE_STRATEGY(strategy)) {
                if (collate_is_c)
                    strategy -= SPG_STRATEGY_ADDITION;
                else
                    continue; // Must traverse entire tree
            }

            // Compare reconstructed value with query
            int r = memcmp(VARDATA(reconstrText), VARDATA_ANY(inText),
                          Min(inSize, thisLen));

            // Apply comparison strategy
            switch (strategy) {
                case BTLessStrategyNumber:
                case BTLessEqualStrategyNumber:
                    if (r > 0) res = false;
                    break;
                case BTEqualStrategyNumber:
                    if (r != 0 || inSize < thisLen) res = false;
                    break;
                case BTGreaterEqualStrategyNumber:
                case BTGreaterStrategyNumber:
                    if (r < 0) res = false;
                    break;
                case RTPrefixStrategyNumber:
                    if (r != 0) res = false;
                    break;
                default:
                    elog(ERROR, "unrecognized strategy number: %d", strategy);
            }

            if (!res) break;
        }

        // Add qualifying node to output
        if (res) {
            out->nodeNumbers[out->nNodes] = i;
            out->levelAdds[out->nNodes] = thisLen - in->level;
            SET_VARSIZE(reconstrText, VARHDRSZ + thisLen);
            out->reconstructedValues[out->nNodes] =
                datumCopy(PointerGetDatum(reconstrText), false, -1);
            out->nNodes++;
        }
    }

    PG_RETURN_VOID();
}
```