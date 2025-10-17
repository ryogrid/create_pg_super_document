# gincost_scalararrayopexpr

## Location
[src/backend/utils/adt/selfuncs.c:7533-7648](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L7533-L7648)

## Overview
Estimates the number of index terms that need to be searched for a GIN index clause involving a ScalarArrayOpExpr (e.g., `column = ANY(array)`).

## Definition
static bool gincost_scalararrayopexpr(PlannerInfo *root, IndexOptInfo *index, int indexcol, ScalarArrayOpExpr *clause, double numIndexEntries, GinQualCounts *counts)

## Detailed Description
The gincost_scalararrayopexpr function handles cost estimation for ScalarArrayOpExpr clauses in GIN indexes, such as `column = ANY(ARRAY[val1, val2, val3])`. Since each array element will result in a separate index scan at runtime, the function processes each array element individually using gincost_pattern, then averages the costs across all satisfiable array elements. It decomposes the array constant, iterates through each non-null element, calculates individual costs, and accumulates the results. For full scan cases, it assumes every index entry would be examined. The function multiplies the arrayScans count by the number of satisfiable elements to account for multiple index scans.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and statistics
- `index`: IndexOptInfo structure containing information about the GIN index
- `indexcol`: Column number within the index being queried
- `clause`: ScalarArrayOpExpr representing the array operation clause being analyzed
- `numIndexEntries`: Estimated total number of entries in the index
- `counts`: GinQualCounts structure to be updated with cost estimation data

## Dependencies
- Functions called/Symbols referenced:
  - lsecond
  - [estimate_expression_value](../e/estimate_expression_value.md)
  - [RelabelType](../R/RelabelType.md)
  - [estimate_array_length](../e/estimate_array_length.md)
  - DatumGetArrayTypeP
  - [get_typlenbyvalalign](get_typlenbyvalalign.md)
  - ARR_ELEMTYPE
  - [deconstruct_array](../d/deconstruct_array.md)
  - [gincost_pattern](gincost_pattern.md)
  - IsA (macro)
- Called from (representative examples):
  - [gincostestimate](gincostestimate.md)

## Notes and Other Information
- Assumes the ScalarArrayOpExpr uses OR semantics (useOr must be true)
- Handles non-constant arrays by falling back to array length estimation
- Ignores null array elements as they cannot match any index entries
- Averages costs across satisfiable array elements to model expected per-scan cost
- For full scan cases, treats it as if every index entry was queried
- Returns false if no array elements produce satisfiable patterns
- Multiplies arrayScans count to reflect that each array element generates a separate index scan
- Skips unsatisfiable patterns when calculating averages but counts them for array scan multiplication

## Simplified Source

```c
static bool gincost_scalararrayopexpr(PlannerInfo *root, IndexOptInfo *index, int indexcol,
                                     ScalarArrayOpExpr *clause, double numIndexEntries,
                                     GinQualCounts *counts) {
    Oid clause_op = clause->opno;
    Node *rightop = (Node *) lsecond(clause->args);
    ArrayType *arrayval;
    int numElems;
    Datum *elemValues;
    bool *elemNulls;
    GinQualCounts arraycounts;
    int numPossible = 0;

    Assert(clause->useOr);

    // Try to reduce array operand to a constant
    rightop = estimate_expression_value(root, rightop);
    if (IsA(rightop, RelabelType))
        rightop = (Node *) ((RelabelType *) rightop)->arg;

    // Handle non-constant arrays with conservative estimate
    if (!IsA(rightop, Const)) {
        counts->exactEntries++;
        counts->searchEntries++;
        counts->arrayScans *= estimate_array_length(root, rightop);
        return true;
    }

    // Handle null constants - no matches possible
    if (((Const *) rightop)->constisnull)
        return false;

    // Extract array elements
    arrayval = DatumGetArrayTypeP(((Const *) rightop)->constvalue);
    deconstruct_array(arrayval, ARR_ELEMTYPE(arrayval),
                     /* element type info parameters */,
                     &elemValues, &elemNulls, &numElems);

    memset(&arraycounts, 0, sizeof(arraycounts));

    // Process each array element
    for (int i = 0; i < numElems; i++) {
        GinQualCounts elemcounts;

        // Skip null elements
        if (elemNulls[i]) continue;

        memset(&elemcounts, 0, sizeof(elemcounts));

        // Get cost estimate for this element
        if (gincost_pattern(index, indexcol, clause_op, elemValues[i], &elemcounts)) {
            numPossible++;

            // Handle full scan case
            if (elemcounts.attHasFullScan[indexcol] && !elemcounts.attHasNormalScan[indexcol]) {
                elemcounts.partialEntries = 0;
                elemcounts.exactEntries = numIndexEntries;
                elemcounts.searchEntries = numIndexEntries;
            }

            // Accumulate costs
            arraycounts.partialEntries += elemcounts.partialEntries;
            arraycounts.exactEntries += elemcounts.exactEntries;
            arraycounts.searchEntries += elemcounts.searchEntries;
        }
    }

    if (numPossible == 0)
        return false;

    // Average costs across satisfiable elements
    counts->partialEntries += arraycounts.partialEntries / numPossible;
    counts->exactEntries += arraycounts.exactEntries / numPossible;
    counts->searchEntries += arraycounts.searchEntries / numPossible;

    // Account for multiple array scans
    counts->arrayScans *= numPossible;

    return true;
}
```

**Core Logic**: Processes array operations like `column = ANY(array)` by extracting array elements, calculating cost estimates for each satisfiable element via gincost_pattern, averaging the costs, and multiplying array scan count by the number of satisfiable elements.