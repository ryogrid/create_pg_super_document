# cost_gather_merge

## Location
[src/backend/optimizer/path/costsize.c:474-548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L474-L548)

## Overview
Calculates the cost estimate for a gather merge path, which merges pre-sorted input streams from parallel workers using a heap-based approach to maintain overall sort order.

## Definition
```c
void cost_gather_merge(GatherMergePath *path, PlannerInfo *root,
                       RelOptInfo *rel, ParamPathInfo *param_info,
                       Cost input_startup_cost, Cost input_total_cost,
                       double *rows)
```

## Detailed Description
The `cost_gather_merge` function determines the total cost of executing a gather merge operation in PostgreSQL's parallel query processing. Unlike a regular gather operation, gather merge maintains the sort order of input streams by using a heap-based merge algorithm. The function calculates costs for heap construction (N*log2(N) comparisons where N is the number of workers plus leader) and heap maintenance (log2(N) comparisons per output tuple).

The costing model accounts for the algorithmic complexity of merging multiple sorted streams, with additional overhead compared to regular gather operations due to the need to coordinate and merge sorted inputs. The function also applies a 5% penalty to communication costs compared to regular gather, reflecting the additional blocking required to maintain sort order.

## Parameters / Member Variables
- `path`: The GatherMergePath structure to populate with cost estimates
- `root`: PlannerInfo containing global planning context and configuration  
- `rel`: The RelOptInfo for the relation being operated upon
- `param_info`: ParamPathInfo for parameterized paths, or NULL for non-parameterized paths
- `input_startup_cost`: Startup cost of the input subpath
- `input_total_cost`: Total cost of the input subpath
- `rows`: Optional pointer to explicit row count estimate that overrides rel and param_info estimates

## Dependencies
- Functions called/Symbols referenced:
  - [GatherMergePath](../G/GatherMergePath.md) (structure)
  - [ParamPathInfo](../P/ParamPathInfo.md) (structure)
  - Cost (type)
  - LOG2 (macro)
  - enable_gathermerge (global variable)
  - disable_cost (global variable)
  - cpu_operator_cost (global variable)
  - parallel_setup_cost (global variable)
  - parallel_tuple_cost (global variable)
- Called from (representative examples):
  - [create_gather_merge_path](create_gather_merge_path.md)

## Notes and Other Information
The function implements a sophisticated costing model that reflects the O(N log N) complexity of heap-based merge operations. The 5% communication cost penalty acknowledges that gather merge requires more coordination than regular gather since it must wait for tuples from all workers to maintain sort order. The algorithm assumes each tuple comparison costs twice the standard CPU operator cost to account for the overhead of heap maintenance operations.