# brin_minmax_union

## Location
[src/backend/access/brin/brin_minmax.c:208-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax.c#L208-L260)

## Overview
Merges two BRIN minmax summary values by computing the union of their ranges, updating the first summary to encompass both value ranges.

## Definition
```c
Datum brin_minmax_union(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a union operation on two BRIN minmax summaries, which is essential during index maintenance operations like page splits, merges, or when consolidating summary information from multiple sources. The function compares the minimum and maximum values from both summaries and updates the first summary to represent the combined range.

The union operation works by:
1. Comparing the minimum values and taking the smaller one
2. Comparing the maximum values and taking the larger one
3. Updating the first summary (col_a) with the expanded range if necessary

This ensures that the resulting summary correctly represents the union of all values that were summarized in both input summaries, maintaining the correctness of the BRIN index's range information.

## Parameters / Member Variables
- `bdesc` (BrinDesc *): BRIN descriptor containing index metadata
- `col_a` (BrinValues *): First summary to be updated with the union result
- `col_b` (BrinValues *): Second summary to merge (remains unchanged)

## Dependencies
- Functions called/Symbols referenced:
  - [BrinDesc](../B/BrinDesc.md) (structure type)
  - [BrinValues](../B/BrinValues.md) (structure type)
  - [minmax_get_strategy_procinfo](../m/minmax_get_strategy_procinfo.md) (function to get comparison procedures)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (function call with collation)
  - [datumCopy](../d/datumCopy.md) (function for copying datum values)
  - BTLessStrategyNumber (B-tree strategy constant)
  - BTGreaterStrategyNumber (B-tree strategy constant)
  - PG_GET_COLLATION (macro to get collation)
  - PG_RETURN_VOID (macro to return void)

- Called from (representative examples):
  - No direct callers found (likely called via function manager during index operations)

## Notes and Other Information
- Both input summaries must be for the same attribute (asserted with col_a->bv_attno == col_b->bv_attno)
- Neither input summary should be all-nulls (asserted in the function)
- The function modifies col_a in place while leaving col_b unchanged
- Proper memory management is performed, freeing old values before copying new ones for pass-by-reference types
- The function returns void since the result is stored in the first parameter
- This operation is commutative: union(A,B) produces the same range as union(B,A), though the function updates only the first parameter