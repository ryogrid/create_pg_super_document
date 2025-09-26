# cost_gather

## Location
[src/backend/optimizer/path/costsize.c:436-473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L436-L473)

## Overview
Calculates the cost estimate for a gather path, which is used in PostgreSQL's parallel query execution to combine results from multiple parallel worker processes.

## Definition

```c
void
cost_gather(GatherPath *path, PlannerInfo *root,
			RelOptInfo *rel, ParamPathInfo *param_info,
			double *rows)
```
## Detailed Description
The  function determines the total cost of executing a gather operation in PostgreSQL's parallel query processing. A gather path represents the coordination point where the main process collects results from parallel worker processes. The function calculates both startup and run costs by considering the underlying subpath costs and adding parallel-specific overhead costs including setup time and per-tuple communication costs.

The function sets the row estimate for the path based on the provided parameters, with precedence given to explicit row estimates, then parameterized path info, and finally the relation's row estimate. The cost calculation includes the subpath's startup and total costs, plus additional parallel setup costs and per-tuple communication overhead.

## Parameters / Member Variables
- : The GatherPath structure to populate with cost estimates
- : PlannerInfo containing global planning context and configuration
- : The RelOptInfo for the relation being operated upon  
- : ParamPathInfo for parameterized paths, or NULL for non-parameterized paths
- : Optional pointer to explicit row count estimate that overrides rel and param_info estimates

## Dependencies
- Functions called/Symbols referenced:
  - [GatherPath](../G/GatherPath.md) (structure)
  - [ParamPathInfo](../P/ParamPathInfo.md) (structure)
  - Cost (type)
  - parallel_setup_cost (global variable)
  - parallel_tuple_cost (global variable)
- Called from (representative examples):
  - [create_gather_path](create_gather_path.md)

## Notes and Other Information
The function adds two main components to the base subpath cost: parallel_setup_cost for the overhead of coordinating parallel workers, and parallel_tuple_cost multiplied by the number of rows for the per-tuple communication overhead. These costs reflect the real-world overhead of parallel query execution in PostgreSQL.