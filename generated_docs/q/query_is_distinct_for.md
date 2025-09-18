# query_is_distinct_for

## Location
src/backend/optimizer/plan/analyzejoins.c: 995 - 1143

## Overview
Determines whether a query is guaranteed to never return duplicate rows for a specified set of output columns, considering various SQL constructs that can ensure distinctness.

## Definition
```c
bool query_is_distinct_for(Query *query, List *colnos, List *opids)
```

## Detailed Description
This function performs a comprehensive analysis to determine if the given query structure guarantees distinctness for the specified columns. It examines various SQL constructs that can provide uniqueness guarantees:

1. **DISTINCT clauses**: Explicit DISTINCT or DISTINCT ON ensures uniqueness if all DISTINCT columns appear in the target column set and operators are compatible
2. **GROUP BY clauses**: Grouping without grouping sets guarantees uniqueness for the grouped columns
3. **Grouping sets**: Special handling for empty grouping sets (single row result) vs. multiple sets
4. **Aggregation**: Queries with aggregates or HAVING but no GROUP BY produce at most one row
5. **Set operations**: UNION/INTERSECT/EXCEPT without ALL guarantee whole-row uniqueness

The function also handles edge cases like set-returning functions (SRFs) which can break distinctness guarantees by producing multiple rows from single inputs.

Operator compatibility is checked using `equality_ops_are_compatible()` to ensure that the upper-level equality operators would consider values distinct in the same way as the subquery's operators.

## Parameters / Member Variables
- `query`: Pointer to the not-yet-planned subquery to analyze  
- `colnos`: List of integer output column numbers (resno's) to check for distinctness
- `opids`: List of equality operator OIDs corresponding to each column for compatibility checking

## Dependencies
- Functions called/Symbols referenced:
  - list_length
  - get_sortgroupclause_tle
  - distinct_col_search  
  - equality_ops_are_compatible
  - castNode
  - list_head
  - lnext
  - linitial
- Data structures used:
  - SortGroupClause
  - TargetEntry
  - GroupingSet
  - SetOperationStmt
- Called from:
  - rel_is_distinct_for (src/backend/optimizer/plan/analyzejoins.c:939)
  - create_unique_path (src/backend/optimizer/util/pathnode.c:1762)

## Notes and Other Information
- Must be kept in sync with `query_supports_distinctness()` for optimization consistency
- Handles cross-type operators by checking btree/hash opfamily membership for compatibility
- SRFs in the target list break distinctness guarantees unless handled by DISTINCT
- Grouping sets with expressions are considered too complex and punt to false
- Set operations check all non-junk output columns for completeness
- Part of PostgreSQL's advanced query optimization for join elimination and unique path generation