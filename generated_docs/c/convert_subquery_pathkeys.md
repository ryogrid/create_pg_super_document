# convert_subquery_pathkeys

## Location
[src/backend/optimizer/path/pathkeys.c:1052-1248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L1052-L1248)

## Overview
Converts a subquery's output pathkeys into equivalent pathkeys in the context of the outer query, handling volatile expressions and multiple equivalence class representations.

## Definition


## Detailed Description
This function performs the complex task of translating pathkeys from a subquery's internal representation to pathkeys that are meaningful in the outer query's context. It handles two main cases:

1. **Volatile EquivalenceClasses**: These must come from ORDER BY clauses and are matched directly to specific targetlist entries using sortref information.

2. **Non-volatile EquivalenceClasses**: These may contain multiple equivalent expressions and require scoring to select the best representation in the outer query context.

For non-volatile classes, the function evaluates each possible representation by counting equivalence class members and checking alignment with outer query pathkeys. It preserves the raw ordering information rather than truncating it, which helps with merge join direction decisions.

The conversion process stops when a subquery pathkey cannot be represented in the outer query, as subsequent pathkeys would also be unusable.

## Parameters / Member Variables
- : PlannerInfo containing the outer query's planning context and equivalence classes
- : RelOptInfo representing the subquery relation in the outer query
- : List of PathKey objects representing the subquery's output ordering
- : The subquery's target list for matching expressions to outer query variables

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgroupref_tle](../g/get_sortgroupref_tle.md) (to find targetlist entries by sortref)
  - [find_var_for_subquery_tle](../f/find_var_for_subquery_tle.md) (to map subquery outputs to outer query variables)
  - [get_eclass_for_sort_expr](../g/get_eclass_for_sort_expr.md) (to find or create equivalence classes)
  - [make_canonical_pathkey](../m/make_canonical_pathkey.md) (to create standardized pathkeys)
  - [canonicalize_ec_expression](canonicalize_ec_expression.md) (to normalize expressions for comparison)
  - [equal](../e/equal.md) (for expression equality testing)
  - [pathkey_is_redundant](../p/pathkey_is_redundant.md) (to eliminate duplicate ordering information)
- Called from (representative examples):
  - [set_subquery_pathlist](../s/set_subquery_pathlist.md)
  - [set_cte_pathlist](../s/set_cte_pathlist.md)
  - [build_setop_child_paths](../b/build_setop_child_paths.md)

## Notes and Other Information
- Intentionally preserves raw ordering information instead of truncating useless pathkeys
- Uses a scoring system to select the best representation when multiple options exist
- Handles volatile expressions specially due to their ORDER BY clause origins
- Essential for subquery optimization and proper merge join planning
- Part of PostgreSQL's pathkey propagation system for maintaining sort order information across query levels