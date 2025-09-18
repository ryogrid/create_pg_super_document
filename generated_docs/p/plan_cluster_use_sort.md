# plan_cluster_use_sort

## Location
src/backend/optimizer/plan/planner.c: 6738 - 6858

## Overview
Uses the planner to determine the optimal execution strategy for CLUSTER command by comparing costs of index scan versus sequential scan plus sort.

## Definition


## Detailed Description
The  function performs cost-based optimization to decide how CLUSTER should implement table reorganization. Given a table and its btree index, it compares two strategies:

1. **Index scan approach**: Scan tuples in index order directly
2. **Sequential scan + sort approach**: Read all tuples sequentially, then sort them

The function creates a minimal planner context and builds cost estimates for both approaches. It considers factors like:
- Index expression evaluation costs (doubled for sort comparisons)
- Sequential scan costs
- Sort operation costs using maintenance_work_mem
- Index scan costs including any index expressions

The function returns true if sorting is cheaper, false if index scanning is more cost-effective.

## Parameters / Member Variables
- : Object ID of the table to be clustered
- : Object ID of the btree index to cluster on (assumed to already be validated as btree)

## Dependencies
- Functions called/Symbols referenced:
  - , , , ,  - Planner data structures
  -  - [Node](../N/Node.md) creation utility
  -  - Sets up relation arrays for planning
  -  - Creates RelOptInfo for the table
  -  - Estimates tuple width
  -  - Evaluates expression costs
  -  - Creates sequential scan path
  -  - Estimates sort operation cost
  -  - Creates index scan path
- Called from (representative examples):
  -  - During CLUSTER command execution

## Notes and Other Information
- Requires caller to hold appropriate locks on the table
- Short-circuits to sorting if  is disabled
- Handles cases where target index is not usable (not reached indcheckxmin horizon, system index being ignored)
- Uses  for sort cost estimation since CLUSTER is a maintenance operation
- Creates minimal planner state rather than full query planning infrastructure
- Index expression costs are doubled in sort comparison because tuplesort re-evaluates expressions
- Considers only btree indexes as input (validated by caller)
- Returns true (use sort) as fallback when index is not available for use