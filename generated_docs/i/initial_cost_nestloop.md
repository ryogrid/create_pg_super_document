# initial_cost_nestloop

## Location
[src/backend/optimizer/path/costsize.c:3233-3307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L3233-L3307)

## Overview
Provides a preliminary estimate of the cost of a nestloop join path, producing lower-bound estimates to quickly evaluate path viability before detailed costing.

## Definition

```c
void
initial_cost_nestloop(PlannerInfo *root, JoinCostWorkspace *workspace,
					  JoinType jointype,
					  Path *outer_path, Path *inner_path,
					  JoinPathExtraData *extra)
```
## Detailed Description
This function performs the first phase of nested loop join cost estimation in PostgreSQL's query planner. It quickly produces lower-bound estimates of startup and total costs to determine if a proposed nested loop path should be considered further. The function:

1. **Calculates rescan costs**: Estimates the cost to rescan the inner relation multiple times using cost_rescan.

2. **Computes startup costs**: Sums both outer and inner paths' startup costs since both must be initialized before returning tuples.

3. **Estimates run costs**: Includes the outer path's run cost plus inner rescan startup costs for each outer row beyond the first.

4. **Handles special join types**: For SEMI/ANTI joins or when inner relation is unique, defers detailed cost calculations to final_cost_nestloop since the executor stops after the first match.

5. **Normal join processing**: For regular joins, includes full inner relation scan costs multiplied by the number of outer rows.

The function deliberately excludes CPU-cost considerations and detailed join qualification analysis to maintain speed, leaving these expensive calculations for final_cost_nestloop.

## Parameters / Member Variables
- : PlannerInfo structure containing planner context and statistics
- : JoinCostWorkspace structure to be filled with preliminary cost estimates and intermediate data
- : Type of join operation (INNER, LEFT, RIGHT, FULL, SEMI, ANTI, etc.)
- : Path representing the outer input to the join
- : Path representing the inner input to the join  
- : JoinPathExtraData containing miscellaneous join information

## Dependencies
- Functions called/Symbols referenced:
  - [cost_rescan](../c/cost_rescan.md)
  - [JoinCostWorkspace](../J/JoinCostWorkspace.md)
  - JoinType
  - [JoinPathExtraData](../J/JoinPathExtraData.md)
  - Cost
  - JOIN_SEMI
  - JOIN_ANTI
- Called from (representative examples):
  - [try_nestloop_path](../t/try_nestloop_path.md)
  - [try_partial_nestloop_path](../t/try_partial_nestloop_path.md)

## Notes and Other Information
- This is the first phase of a two-phase nested loop costing process
- CPU costs and join qualification analysis are deferred to final_cost_nestloop for performance
- For SEMI/ANTI joins or unique inner relations, detailed run cost calculations are postponed
- The function produces conservative lower-bound estimates to enable quick path pruning
- Workspace structure preserves intermediate data for use by final_cost_nestloop
- The division of labor between initial and final costing represents a speed vs. accuracy tradeoff

## Simplified Source

```c
void
initial_cost_nestloop(PlannerInfo *root, JoinCostWorkspace *workspace,
                      JoinType jointype,
                      Path *outer_path, Path *inner_path,
                      JoinPathExtraData *extra)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    double outer_path_rows = outer_path->rows;
    Cost inner_rescan_start_cost;
    Cost inner_rescan_total_cost;
    Cost inner_run_cost;
    Cost inner_rescan_run_cost;

    // Calculate cost to rescan inner relation
    cost_rescan(root, inner_path, &inner_rescan_start_cost, &inner_rescan_total_cost);

    // Startup costs: both outer and inner paths must be initialized
    startup_cost += outer_path->startup_cost + inner_path->startup_cost;

    // Run costs: outer path plus rescan startup for each additional outer row
    run_cost += outer_path->total_cost - outer_path->startup_cost;
    if (outer_path_rows > 1)
        run_cost += (outer_path_rows - 1) * inner_rescan_start_cost;

    inner_run_cost = inner_path->total_cost - inner_path->startup_cost;
    inner_rescan_run_cost = inner_rescan_total_cost - inner_rescan_start_cost;

    if (jointype == JOIN_SEMI || jointype == JOIN_ANTI || extra->inner_unique)
    {
        // SEMI/ANTI joins or unique inner: defer detailed cost calculations
        // Executor stops after first match, needs join qual inspection
        workspace->inner_run_cost = inner_run_cost;
        workspace->inner_rescan_run_cost = inner_rescan_run_cost;
    }
    else
    {
        // Normal case: scan whole inner relation for each outer row
        run_cost += inner_run_cost;
        if (outer_path_rows > 1)
            run_cost += (outer_path_rows - 1) * inner_rescan_run_cost;
    }

    // Store results for final_cost_nestloop
    workspace->startup_cost = startup_cost;
    workspace->total_cost = startup_cost + run_cost;
    workspace->run_cost = run_cost;
}
```