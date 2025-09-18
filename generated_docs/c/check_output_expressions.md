# check_output_expressions

## Location
[src/backend/optimizer/path/allpaths.c:3707-3778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L3707-L3778)

## Overview
Analyzes each output column of a subquery's target list to identify and flag columns that are unsafe for qual pushdown references, marking specific safety violations for later consultation.

## Definition
static void check_output_expressions(Query *subquery, pushdown_safety_info *safetyInfo)

## Detailed Description
This function performs granular safety analysis of subquery output columns to determine which columns can be safely referenced by pushed-down quals. Rather than rejecting entire subqueries, it marks individual columns as unsafe for specific reasons, allowing quals that don't reference problematic columns to still be pushed down.

The function checks four specific safety conditions for each non-resjunk target list entry:

1. **Set-returning functions**: Columns containing functions that return sets are marked UNSAFE_HAS_SET_FUNC to prevent introducing set-returning functions into WHERE/HAVING clauses through qual pushdown.

2. **Volatile functions**: Columns with volatile functions are marked UNSAFE_HAS_VOLATILE_FUNC to avoid strange results from multiple evaluation of volatile functions in different contexts.

3. **DISTINCT ON compliance**: For subqueries using DISTINCT ON, columns not listed in the DISTINCT ON clause are marked UNSAFE_NOTIN_DISTINCTON_CLAUSE since pushing quals on these columns could change the result set.

4. **Window function partitioning**: For subqueries with window functions, columns not present in ALL window PARTITION BY clauses are marked UNSAFE_NOTIN_PARTITIONBY_CLAUSE to maintain window function semantics.

The unsafeFlags array is indexed by column position (resno) and uses bitwise flags to track multiple concurrent safety violations for the same column.

## Parameters / Member Variables
- : Query structure whose target list expressions need safety analysis
- : Structure containing unsafeFlags array to record per-column safety violations

## Dependencies
- Functions called/Symbols referenced:
  - [expression_returns_set](../e/expression_returns_set.md): Determines if an expression returns multiple rows/values
  - [contain_volatile_functions](contain_volatile_functions.md): Detects presence of volatile functions in expressions
  - [targetIsInSortList](../t/targetIsInSortList.md): Checks if a target entry appears in DISTINCT ON clause
  - [targetIsInAllPartitionLists](../t/targetIsInAllPartitionLists.md): Verifies target entry appears in all window PARTITION BY clauses
- Called from (representative examples):
  - [subquery_is_pushdown_safe](../s/subquery_is_pushdown_safe.md): During leaf query safety analysis

## Notes and Other Information
- Implements fine-grained column-level safety tracking rather than query-level rejection
- Uses bitwise flags (UNSAFE_HAS_SET_FUNC, UNSAFE_HAS_VOLATILE_FUNC, etc.) for efficient storage
- Skips resjunk columns as they are not user-visible in query results
- Works with qual_is_pushdown_safe() which consults these flags during actual pushdown decisions
- Critical for maximizing optimization opportunities while preserving correctness
- Enables partial qual pushdown when only some columns have safety issues
- Supports complex subqueries with multiple concurrent safety concerns per column