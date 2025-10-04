# spgist_name_inner_consistent

## Location
[src/test/modules/spgist_name_ops/spgist_name_ops.c:266-398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/spgist_name_ops/spgist_name_ops.c#L266-L398)

## Overview
Implements the inner consistent function for SP-GiST name operator class, evaluating which child nodes should be visited during index traversal based on search predicates.

## Definition
```c
Datum spgist_name_inner_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of the SP-GiST (Space-Partitioned Generalized Search Tree) implementation for PostgreSQL's name data type. It determines which child nodes of an inner tuple should be visited during index traversal by reconstructing partial values and comparing them against search conditions.

The function reconstructs text values by combining:
1. Previously reconstructed value from parent levels
2. Prefix data from the current tuple (if any)  
3. Node labels from child nodes

For each child node, it performs string comparison operations against all scan keys using the appropriate B-tree strategy (less than, equal, greater than, etc.) to determine consistency.

## Parameters / Member Variables
- : Input structure () containing:
  - : Value reconstructed from parent levels
  - : Length of previously reconstructed value
  - : Whether tuple has prefix data
  - : Prefix data if present
  - : Number of child nodes
  - : Labels for each child node
  - : Number of scan keys
  - : Array of scan key conditions
- : Output structure () to populate with:
  - : Array of consistent child node numbers
  - : Array of level increments for each node
  - : Array of reconstructed values
  - : Number of consistent nodes found

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  - Strategy numbers (, , etc.)
- Called from (representative examples):
  - Referenced by  function

## Notes and Other Information
- Handles dummy node labels (values ≤ 0) by excluding them from reconstructed data
- Performs non-collation-aware string comparisons using
- Assumes reconstructed values use long varlena format (not toasted or short headers)
- Returns void but populates output structure with results
- Part of test module demonstrating SP-GiST operator class implementation for name types

## Simplified Source

```c
Datum spgist_name_inner_consistent(PG_FUNCTION_ARGS) {
    spgInnerConsistentIn *in = (spgInnerConsistentIn *) PG_GETARG_POINTER(0);
    spgInnerConsistentOut *out = (spgInnerConsistentOut *) PG_GETARG_POINTER(1);

    // Reconstruct value from parent data, prefix, and node labels
    text *reconstructedValue = (text *) DatumGetPointer(in->reconstructedValue);
    int maxReconstrLen = in->level + 1;

    if (in->hasPrefix) {
        text *prefixText = DatumGetTextPP(in->prefixDatum);
        int prefixSize = VARSIZE_ANY_EXHDR(prefixText);
        maxReconstrLen += prefixSize;
    }

    text *reconstrText = palloc(VARHDRSZ + maxReconstrLen);
    SET_VARSIZE(reconstrText, VARHDRSZ + maxReconstrLen);

    // Copy parent data and prefix into reconstructed text
    if (in->level)
        memcpy(VARDATA(reconstrText), VARDATA(reconstructedValue), in->level);
    if (in->hasPrefix) {
        text *prefixText = DatumGetTextPP(in->prefixDatum);
        memcpy(((char *) VARDATA(reconstrText)) + in->level,
               VARDATA_ANY(prefixText), VARSIZE_ANY_EXHDR(prefixText));
    }

    // Allocate output arrays
    out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
    out->levelAdds = (int *) palloc(sizeof(int) * in->nNodes);
    out->reconstructedValues = (Datum *) palloc(sizeof(Datum) * in->nNodes);
    out->nNodes = 0;

    // Check each child node for consistency with scan keys
    for (int i = 0; i < in->nNodes; i++) {
        int16 nodeChar = DatumGetInt16(in->nodeLabels[i]);
        int thisLen = maxReconstrLen - 1;
        bool consistent = true;

        // Handle non-dummy node labels by adding character to reconstructed text
        if (nodeChar > 0) {
            ((unsigned char *) VARDATA(reconstrText))[maxReconstrLen - 1] = nodeChar;
            thisLen = maxReconstrLen;
        }

        // Test against all scan key conditions
        for (int j = 0; j < in->nkeys && consistent; j++) {
            StrategyNumber strategy = in->scankeys[j].sk_strategy;
            Name inName = DatumGetName(in->scankeys[j].sk_argument);
            char *inStr = NameStr(*inName);
            int inSize = strlen(inStr);

            int comparison = memcmp(VARDATA(reconstrText), inStr, Min(inSize, thisLen));

            // Apply strategy-specific comparison logic
            switch (strategy) {
                case BTLessStrategyNumber:
                case BTLessEqualStrategyNumber:
                    if (comparison > 0) consistent = false;
                    break;
                case BTEqualStrategyNumber:
                    if (comparison != 0 || inSize < thisLen) consistent = false;
                    break;
                case BTGreaterEqualStrategyNumber:
                case BTGreaterStrategyNumber:
                    if (comparison < 0) consistent = false;
                    break;
            }
        }

        // Add consistent nodes to output
        if (consistent) {
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