# find_simplified_clause

## Location
[src/backend/utils/adt/rangetypes.c:2786-2907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2786-L2907)

## Overview
A static function that attempts to simplify range containment expressions by converting them into simpler boundary comparison expressions when the range is a constant.

## Definition

```c
static Node *
find_simplified_clause(PlannerInfo *root, Expr *rangeExpr, Expr *elemExpr)
```
## Detailed Description
This function is a core optimization component for PostgreSQL's range type query planning. It analyzes range containment operations (element contained by range, and range contains element) and attempts to transform them into simpler boundary comparisons when the range operand is a non-null constant. The function deserializes the constant range, examines its bounds, and constructs equivalent comparison expressions using the range's lower and/or upper bounds. It includes cost-based optimizations to avoid creating expensive expressions that would evaluate the element expression multiple times, particularly for volatile or computationally expensive expressions.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planning context and cost parameters
- `*rangeExpr`: Expression representing the range operand (must be a constant for optimization)
- `*elemExpr`: Expression representing the element to test for containment
## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - RangeTypeGetOid
  - [range_deserialize](../r/range_deserialize.md)
  - [makeBoolConst](../m/makeBoolConst.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [contain_subplans](../c/contain_subplans.md)
  - [cost_qual_eval_node](../c/cost_qual_eval_node.md)
  - [build_bound_expr](../b/build_bound_expr.md)
  - copyObject
  - [make_andclause](../m/make_andclause.md)
  - list_make2
  - TYPECACHE_RANGE_INFO
- Called from (representative examples):
  - [elem_contained_by_range_support](../e/elem_contained_by_range_support.md)
  - [range_contains_elem_support](../r/range_contains_elem_support.md)

## Notes and Other Information
This function implements sophisticated query optimization by replacing complex range operations with simpler comparisons. It handles special cases like empty ranges (always false) and infinite ranges (always true). For finite ranges, it carefully evaluates whether creating boundary comparisons is beneficial, considering both expression volatility and computational cost. When both bounds are present, it uses a cost threshold of 10 * cpu_operator_cost to determine if the optimization should be applied. The function is essential for efficient range query execution in PostgreSQL's query planner.