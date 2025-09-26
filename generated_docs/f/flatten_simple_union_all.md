# flatten_simple_union_all

## Location
[src/backend/optimizer/prep/prepjointree.c:2814-2932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L2814-L2932)

## Overview
Optimizes top-level UNION ALL structures by converting them into appendrel relationships, enabling more intelligent query processing than the general setops case.

## Definition
```c
void flatten_simple_union_all(PlannerInfo *root)
```

## Detailed Description
This function attempts to flatten a query's setOperations tree that consists entirely of simple UNION ALL operations into an append relation. The optimization transforms the complex setops tree structure into a simpler appendrel format that the PostgreSQL planner can process more efficiently.

The function performs several key steps:
1. Validates that the query has only UNION ALL operations with identical column types
2. Locates the leftmost leaf query in the setops tree (which upper query Vars reference)
3. Creates a copy of the leftmost RTE to represent it as an appendrel member
4. Modifies the original RTE to mark it as an appendrel parent
5. Restructures the query to remove setOperations and add proper FROM clause
6. Applies subquery pullup optimizations to the leaf queries

This optimization is most effective for top-level queries, as subqueries would typically be flattened earlier by pull_up_subqueries. However, it handles special cases like subqueries containing ORDER BY clauses that cannot be processed in the earlier optimization phase.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the query tree and planner state information

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - [is_simple_union_all_recurse](../i/is_simple_union_all_recurse.md)
  - rt_fetch
  - copyObject
  - [lappend](../l/lappend.md)
  - [list_length](../l/list_length.md)
  - makeNode
  - list_make1
  - [pull_up_union_leaf_queries](../p/pull_up_union_leaf_queries.md)
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md)

## Notes and Other Information
- Cannot optimize recursive UNION operations (checked via root->hasRecursion)
- Requires all UNION operations to be UNION ALL with identical column types
- The transformation preserves the original query semantics while enabling more efficient execution plans
- This function is declared in src/include/optimizer/prep.h and is part of the query preparation phase
- The optimization restructures the query tree significantly, converting from a setops-based representation to an appendrel-based one