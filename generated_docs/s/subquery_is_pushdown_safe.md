# subquery_is_pushdown_safe

## Location
[src/backend/optimizer/path/allpaths.c:3582-3637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L3582-L3637)

## Overview
Determines whether it is safe to push down WHERE clauses (quals) into a subquery by checking for various SQL constructs that could change query semantics if quals are pushed down.

## Definition
static bool subquery_is_pushdown_safe(Query *subquery, Query *topquery, pushdown_safety_info *safetyInfo)

## Detailed Description
This function is a critical component of PostgreSQL's query optimization that analyzes whether quals can be safely pushed down into subqueries for better performance. The function performs comprehensive safety checks to ensure that pushing quals down will not change the query's semantic behavior or results.

The function checks six main safety conditions:

1. **LIMIT clauses**: Quals cannot be pushed if the subquery has LIMIT/OFFSET, as this could change which rows are returned
2. **EXCEPT operations**: EXCEPT and EXCEPT ALL are incompatible with qual pushdown as it could alter set operation results  
3. **DISTINCT operations**: Volatile quals cannot be pushed into DISTINCT subqueries as they should evaluate once per distinct row, not per original row
4. **Window functions**: Volatile quals cannot be pushed down as they might change partition contents and affect window function results
5. **Set-returning functions**: Volatile quals cannot be pushed below SRFs as this changes the number of evaluations
6. **Grouping sets**: No quals can be pushed into subqueries with grouping sets due to constant-folding issues with potentially nullable grouping columns

For leaf queries, it also checks output expressions for safety and marks unsafe columns in the safetyInfo structure for later consultation by qual_is_pushdown_safe().

## Parameters / Member Variables
- : The specific component query being checked for qual pushdown safety
- : The top-level query component of a set-operations tree (same as subquery if no set-ops involved)
- : Structure to record safety information and unsafe column flags for later qual evaluation

## Dependencies
- Functions called/Symbols referenced:
  - [check_output_expressions](../c/check_output_expressions.md): Analyzes target list expressions for pushdown safety
  - [recurse_pushdown_safe](../r/recurse_pushdown_safe.md): Recursively checks set operation components
  - [compare_tlist_datatypes](../c/compare_tlist_datatypes.md): Validates data type compatibility in set operations
  - castNode: Safe node type casting with assertion
- Called from (representative examples):
  - [set_subquery_pathlist](set_subquery_pathlist.md): During subquery path planning
  - [recurse_pushdown_safe](../r/recurse_pushdown_safe.md): For recursive set operation checking

## Notes and Other Information
- Part of PostgreSQL's qual pushdown optimization infrastructure
- Handles both simple subqueries and complex set operations (UNION, INTERSECT, EXCEPT)
- Marks volatile-unsafe columns rather than rejecting entire subqueries when possible
- Accepts some theoretical risks with DISTINCT and window functions for performance benefits
- Critical for maintaining SQL semantic correctness during optimization
- Works in conjunction with qual_is_pushdown_safe() for final qual evaluation
- Supports complex nested set operations through recursive safety checking

## Simplified Source

```c
static bool
subquery_is_pushdown_safe(Query *subquery, Query *topquery,
                         pushdown_safety_info *safetyInfo)
{
    SetOperationStmt *topop;

    // Check 1: Cannot push quals if LIMIT/OFFSET present
    if (subquery->limitOffset != NULL || subquery->limitCount != NULL)
        return false;

    // Check 6: Cannot push quals if grouping sets present
    if (subquery->groupClause && subquery->groupingSets)
        return false;

    // Check 3,4,5: Mark as volatile-unsafe if DISTINCT, window funcs, or SRFs
    if (subquery->distinctClause ||
        subquery->hasWindowFuncs ||
        subquery->hasTargetSRFs)
        safetyInfo->unsafeVolatile = true;

    // For leaf queries, check output expressions for safety
    if (subquery->setOperations == NULL)
        check_output_expressions(subquery, safetyInfo);

    // Handle top-level vs set operation component logic
    if (subquery == topquery)
    {
        // Top level: recursively check set operation components
        if (subquery->setOperations != NULL)
            if (!recurse_pushdown_safe(subquery->setOperations, topquery,
                                     safetyInfo))
                return false;
    }
    else
    {
        // Set operation component: validate structure and types
        if (subquery->setOperations != NULL)
            return false;
        topop = castNode(SetOperationStmt, topquery->setOperations);
        compare_tlist_datatypes(subquery->targetList,
                              topop->colTypes,
                              safetyInfo);
    }
    return true;
}
```