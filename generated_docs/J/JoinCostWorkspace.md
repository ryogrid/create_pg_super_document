# JoinCostWorkspace

## Location
[src/include/nodes/pathnodes.h:3335-3358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L3335-L3358)

## Overview
JoinCostWorkspace is a workspace structure used for efficient two-phase cost estimation of join paths, storing preliminary cost estimates and intermediate values to avoid redundant calculations.

## Definition

```c
typedef struct JoinCostWorkspace
{
	/* Preliminary cost estimates --- must not be larger than final ones! */
	Cost		startup_cost;	/* cost expended before fetching any tuples */
	Cost		total_cost;		/* total cost (assuming all tuples fetched) */

	/* Fields below here should be treated as private to costsize.c */
	Cost		run_cost;		/* non-startup cost components */

	/* private for cost_nestloop code */
	Cost		inner_run_cost; /* also used by cost_mergejoin code */
	Cost		inner_rescan_run_cost;

	/* private for cost_mergejoin code */
	Cardinality outer_rows;
	Cardinality inner_rows;
	Cardinality outer_skip_rows;
	Cardinality inner_skip_rows;

	/* private for cost_hashjoin code */
	int			numbuckets;
	int			numbatches;
	Cardinality inner_rows_total;
} JoinCostWorkspace;
```
## Detailed Description
JoinCostWorkspace implements a two-phase cost estimation strategy for join operations to improve performance. The first phase quickly derives a lower bound for join cost, which may be sufficient to reject obviously expensive paths. If the path remains viable, the second phase performs more refined cost calculations using the preliminary values as input. This approach avoids expensive computations for paths that can be eliminated early. The structure contains both public cost estimates and private intermediate values specific to different join algorithms (nested loop, merge join, hash join).

## Parameters / Member Variables
- `startup_cost`: Cost expended before fetching any tuples (preliminary estimate, must not exceed final estimate)
- `total_cost`: Total cost assuming all tuples are fetched (preliminary estimate, must not exceed final estimate)
- `run_cost`: Non-startup cost components (private to costsize.c)
- `inner_run_cost`: Cost of running the inner relation (used by both nested loop and merge join algorithms)
- `inner_rescan_run_cost`: Cost of rescanning the inner relation (private to nested loop costing)
- `outer_rows`: Estimated number of rows from outer relation (private to merge join costing)
- `inner_rows`: Estimated number of rows from inner relation (private to merge join costing)
- `outer_skip_rows`: Number of outer rows that can be skipped (private to merge join costing)
- `inner_skip_rows`: Number of inner rows that can be skipped (private to merge join costing)
- `numbuckets`: Number of hash buckets (private to hash join costing)
- `numbatches`: Number of batches for hash join (private to hash join costing)
- `inner_rows_total`: Total number of inner rows across all batches (private to hash join costing)
## Dependencies
- Functions called/Symbols referenced:
  - Cost
  - Cardinality
- Called from (representative examples):
  - [initial_cost_nestloop](../i/initial_cost_nestloop.md)
  - [final_cost_nestloop](../f/final_cost_nestloop.md)
  - [initial_cost_mergejoin](../i/initial_cost_mergejoin.md)
  - [final_cost_mergejoin](../f/final_cost_mergejoin.md)
  - [initial_cost_hashjoin](../i/initial_cost_hashjoin.md)
  - [final_cost_hashjoin](../f/final_cost_hashjoin.md)
  - [try_nestloop_path](../t/try_nestloop_path.md)
  - [try_mergejoin_path](../t/try_mergejoin_path.md)
  - [try_hashjoin_path](../t/try_hashjoin_path.md)
  - [create_nestloop_path](../c/create_nestloop_path.md)
  - [create_mergejoin_path](../c/create_mergejoin_path.md)
  - [create_hashjoin_path](../c/create_hashjoin_path.md)

## Notes and Other Information
- The two-phase costing strategy is a performance optimization to avoid expensive calculations for paths that will be rejected
- Preliminary cost estimates in the first phase must never be larger than final ones to ensure correct path elimination
- Different join algorithms use different subsets of the workspace fields
- The structure is designed to be private to costsize.c for most fields, with only startup_cost and total_cost being public
- This design pattern allows for efficient cost comparison and path pruning in the query optimizer
- Located in src/include/nodes/pathnodes.h:3335-3358