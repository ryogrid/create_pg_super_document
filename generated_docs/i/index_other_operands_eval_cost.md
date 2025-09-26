# index_other_operands_eval_cost

## Location
[src/backend/utils/adt/selfuncs.c:6556-6609](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L6556-L6609)

## Overview
Computes the total evaluation cost of the comparison operands (non-index-key side) in a list of index qualification expressions, used for index cost estimation during query planning.

## Definition
```c
Cost index_other_operands_eval_cost(PlannerInfo *root, List *indexquals)
```

## Detailed Description
This function analyzes index qualification expressions to determine the computational cost of evaluating the non-index operands (typically the right-hand side of comparison operations). It handles various types of index clauses including simple operator expressions (OpExpr), row comparisons (RowCompareExpr), scalar array operations (ScalarArrayOpExpr), and null tests (NullTest). The function assumes that the index key expression is always on the left side of binary clauses, so it extracts and costs the other operand. For each clause, it uses cost_qual_eval_node to determine both startup and per-tuple costs, then sums these costs since the operands are evaluated once per scan. The function can work with either the output from get_quals_from_indexclauses() or directly with indexorderbys lists.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and cost parameters
- `indexquals`: List of index qualification expressions (may contain RestrictInfo wrappers)

## Dependencies
- Functions called/Symbols referenced:
  - Cost
  - [QualCost](../Q/QualCost.md)
  - [OpExpr](../O/OpExpr.md)
  - lsecond
  - RowCompareExpr
  - [ScalarArrayOpExpr](../S/ScalarArrayOpExpr.md)
  - [NullTest](../N/NullTest.md)
  - nodeTag
  - [cost_qual_eval_node](../c/cost_qual_eval_node.md)
- Called from (representative examples):
  - [genericcostestimate](../g/genericcostestimate.md)
  - [gincostestimate](../g/gincostestimate.md)
  - [brincostestimate](../b/brincostestimate.md)

## Notes and Other Information
This function is a key component in PostgreSQL's cost-based query optimizer, specifically for index access method costing. It differentiates between different expression types that can appear in index clauses and handles RestrictInfo wrappers that may be present around the actual clauses. The function assumes that operands are evaluated just once per scan rather than per row, which is appropriate for index qualification evaluation. For unsupported clause types, it raises an ERROR, ensuring that all expected index clause formats are explicitly handled. The cost calculation includes both startup and per-tuple components, reflecting the complete cost of operand evaluation during index scanning.