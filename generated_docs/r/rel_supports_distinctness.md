# rel_supports_distinctness

## Location
src/backend/optimizer/plan/analyzejoins.c: 806 - 860

## Overview
A fast pre-checking function that determines whether a relation could possibly be proven distinct on some set of columns, serving as an optimization filter before expensive distinctness analysis.

## Definition
```c
static bool rel_supports_distinctness(PlannerInfo *root, RelOptInfo *rel)
```

## Detailed Description
This function performs a lightweight analysis to determine if a relation has the structural properties needed to potentially prove distinctness (uniqueness) on some column set. It serves as a performance optimization by allowing callers to avoid expensive computation when distinctness proofs are impossible.

The function handles different relation types:
1. **Plain relations (RTE_RELATION)**: Checks for suitable unique indexes that are immediately enforced and not partial indexes
2. **Subqueries (RTE_SUBQUERY)**: Delegates to query_supports_distinctness to analyze the subquery structure
3. **Other relation types**: Currently not supported, returns false

The function maintains sync with relation_has_unique_index_for() regarding what constitutes a "suitable" unique index: it must be unique, immediately enforced (not deferrable), and complete (not partial).

## Parameters / Member Variables
- `root`: Pointer to PlannerInfo structure containing planning context
- `rel`: Pointer to RelOptInfo structure representing the relation to analyze

## Dependencies
- Functions called/Symbols referenced:
  - [query_supports_distinctness](../q/query_supports_distinctness.md) (analyzes subquery distinctness potential)
- Called from (representative examples):
  - [join_is_removable](../j/join_is_removable.md) (when checking if joins can be eliminated)
  - [reduce_unique_semijoins](reduce_unique_semijoins.md) (when optimizing semijoin operations)
  - [innerrel_is_unique](../i/innerrel_is_unique.md) (before performing expensive uniqueness analysis)

## Notes and Other Information
- This is a static function within analyzejoins.c, serving as an internal utility
- The function is designed to be fast and avoid expensive operations, as it's used as a filter
- Only base relations (RELOPT_BASEREL) are currently supported for distinctness analysis
- For plain relations, uniqueness can only be proven through unique indexes, not through other mechanisms
- The conditions for "suitable" unique indexes must be kept in sync with relation_has_unique_index_for()
- The function returns false for relation types like functions, values, CTE, etc., as there are no proof rules for these
- This pre-check can significantly improve performance by avoiding unnecessary distinctness computations
- Located in src/backend/optimizer/plan/analyzejoins.c at lines 806-860