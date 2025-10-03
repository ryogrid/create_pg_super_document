# initial_cost_mergejoin

## Location
[src/backend/optimizer/path/costsize.c:3514-3744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L3514-L3744)

## Overview
Provides a preliminary estimate of the cost of a mergejoin path, producing lower-bound estimates to quickly evaluate path viability before detailed costing.

## Definition

```c
void
initial_cost_mergejoin(PlannerInfo *root, JoinCostWorkspace *workspace,
					   JoinType jointype,
					   List *mergeclauses,
					   Path *outer_path, Path *inner_path,
					   List *outersortkeys, List *innersortkeys,
					   JoinPathExtraData *extra)
```
## Detailed Description
This function performs the first phase of merge join cost estimation in PostgreSQL's query planner. It quickly produces lower-bound estimates by:

1. **Scan selectivity analysis**: Uses cached selectivity estimates from the first merge clause to determine what fraction of each input will actually be scanned. Merge joins can terminate early when one input is exhausted (except for full outer joins).

2. **Sort cost calculation**: If either input requires sorting (indicated by non-NULL sortkeys), calculates sorting costs using cost_sort. The function accounts for partial sorting costs based on selectivity estimates.

3. **Input processing estimation**: Estimates startup costs including sort setup and the portion of input that must be read before the first join pair is found. Run costs account for the remaining input that will be processed.

4. **Join type handling**: Adjusts selectivity estimates for different join types:
   - LEFT/ANTI joins: Force outer relation to be fully scanned
   - RIGHT/RIGHT_ANTI joins: Force inner relation to be fully scanned
   - FULL joins: Both relations must be fully processed

5. **Deferred analysis**: Excludes CPU costs and detailed qualification evaluation to maintain speed, leaving these for final_cost_mergejoin.

The function protects against zero row counts and uses clamp_row_est to ensure reasonable estimates.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner context and statistics
- `*workspace`: JoinCostWorkspace structure to be filled with preliminary cost estimates and intermediate data
- `jointype`: Type of join operation (INNER, LEFT, RIGHT, FULL, SEMI, ANTI, etc.)
- `*mergeclauses`: List of join clauses to be used as merge clauses
- `*outer_path`: Path representing the outer input to the join
- `*inner_path`: Path representing the inner input to the join
- `*outersortkeys`: List of sort keys for outer path (NULL if already sorted)
- `*innersortkeys`: List of sort keys for inner path (NULL if already sorted)
- `*extra`: JoinPathExtraData containing miscellaneous join information
## Dependencies
- Functions called/Symbols referenced:
  - [cost_sort](../c/cost_sort.md)
  - [cached_scansel](../c/cached_scansel.md)
  - [clamp_row_est](../c/clamp_row_est.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [JoinCostWorkspace](../J/JoinCostWorkspace.md)
  - JoinType
  - [JoinPathExtraData](../J/JoinPathExtraData.md)
  - [PathKey](../P/PathKey.md)
  - [MergeScanSelCache](../M/MergeScanSelCache.md)
  - Cost
  - JOIN_FULL, JOIN_LEFT, JOIN_ANTI, JOIN_RIGHT, JOIN_RIGHT_ANTI
- Called from (representative examples):
  - [try_mergejoin_path](../t/try_mergejoin_path.md)
  - [try_partial_mergejoin_path](../t/try_partial_mergejoin_path.md)

## Notes and Other Information
- This is the first phase of a two-phase merge join costing process
- Uses cached selectivity results from mergejoinscansel() to avoid expensive recomputation
- [Sort](../S/Sort.md) keys should be NIL when the respective source path is already properly ordered
- CPU costs and detailed join qualification analysis are deferred to final_cost_mergejoin
- Selectivity estimates are readjusted after rounding to maintain accuracy with small input sizes
- The function assumes cost_sort is efficient enough for use in preliminary estimation
- Inner input cost considerations (rescanning, materialization) are partially deferred
- Workspace structure preserves intermediate data for final costing phase

## Simplified Source

```c
void
initial_cost_mergejoin(PlannerInfo *root, JoinCostWorkspace *workspace,
                      JoinType jointype, List *mergeclauses,
                      Path *outer_path, Path *inner_path,
                      List *outersortkeys, List *innersortkeys,
                      JoinPathExtraData *extra)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    double outer_path_rows = outer_path->rows;
    double inner_path_rows = inner_path->rows;
    Cost inner_run_cost;
    double outer_rows, inner_rows, outer_skip_rows, inner_skip_rows;
    Selectivity outerstartsel, outerendsel, innerstartsel, innerendsel;

    // Ensure non-zero row counts for calculations
    if (outer_path_rows <= 0) outer_path_rows = 1;
    if (inner_path_rows <= 0) inner_path_rows = 1;

    // Calculate selectivity estimates from merge clauses
    if (mergeclauses && jointype != JOIN_FULL) {
        RestrictInfo *firstclause = (RestrictInfo *) linitial(mergeclauses);
        List *opathkeys = outersortkeys ? outersortkeys : outer_path->pathkeys;
        List *ipathkeys = innersortkeys ? innersortkeys : inner_path->pathkeys;
        PathKey *opathkey = (PathKey *) linitial(opathkeys);
        PathKey *ipathkey = (PathKey *) linitial(ipathkeys);

        // Get cached selectivity estimates
        MergeScanSelCache *cache = cached_scansel(root, firstclause, opathkey);

        // Determine which side is outer/inner based on relation membership
        if (bms_is_subset(firstclause->left_relids, outer_path->parent->relids)) {
            // Left side is outer
            outerstartsel = cache->leftstartsel;
            outerendsel = cache->leftendsel;
            innerstartsel = cache->rightstartsel;
            innerendsel = cache->rightendsel;
        } else {
            // Left side is inner
            outerstartsel = cache->rightstartsel;
            outerendsel = cache->rightendsel;
            innerstartsel = cache->leftstartsel;
            innerendsel = cache->leftendsel;
        }

        // Adjust for join type constraints
        if (jointype == JOIN_LEFT || jointype == JOIN_ANTI) {
            outerstartsel = 0.0;
            outerendsel = 1.0;
        } else if (jointype == JOIN_RIGHT || jointype == JOIN_RIGHT_ANTI) {
            innerstartsel = 0.0;
            innerendsel = 1.0;
        }
    } else {
        // Default for clauseless or full joins
        outerstartsel = innerstartsel = 0.0;
        outerendsel = innerendsel = 1.0;
    }

    // Convert selectivities to row estimates
    outer_skip_rows = rint(outer_path_rows * outerstartsel);
    inner_skip_rows = rint(inner_path_rows * innerstartsel);
    outer_rows = clamp_row_est(outer_path_rows * outerendsel);
    inner_rows = clamp_row_est(inner_path_rows * innerendsel);

    // Readjust selectivities after rounding
    outerstartsel = outer_skip_rows / outer_path_rows;
    innerstartsel = inner_skip_rows / inner_path_rows;
    outerendsel = outer_rows / outer_path_rows;
    innerendsel = inner_rows / inner_path_rows;

    // Calculate outer path costs
    if (outersortkeys) {
        // Need to sort outer input
        Path sort_path;
        cost_sort(&sort_path, root, outersortkeys, outer_path->total_cost,
                 outer_path_rows, outer_path->pathtarget->width, 0.0, work_mem, -1.0);
        startup_cost += sort_path.startup_cost;
        startup_cost += (sort_path.total_cost - sort_path.startup_cost) * outerstartsel;
        run_cost += (sort_path.total_cost - sort_path.startup_cost) * (outerendsel - outerstartsel);
    } else {
        // Outer input already sorted
        startup_cost += outer_path->startup_cost;
        startup_cost += (outer_path->total_cost - outer_path->startup_cost) * outerstartsel;
        run_cost += (outer_path->total_cost - outer_path->startup_cost) * (outerendsel - outerstartsel);
    }

    // Calculate inner path costs
    if (innersortkeys) {
        // Need to sort inner input
        Path sort_path;
        cost_sort(&sort_path, root, innersortkeys, inner_path->total_cost,
                 inner_path_rows, inner_path->pathtarget->width, 0.0, work_mem, -1.0);
        startup_cost += sort_path.startup_cost;
        startup_cost += (sort_path.total_cost - sort_path.startup_cost) * innerstartsel;
        inner_run_cost = (sort_path.total_cost - sort_path.startup_cost) * (innerendsel - innerstartsel);
    } else {
        // Inner input already sorted
        startup_cost += inner_path->startup_cost;
        startup_cost += (inner_path->total_cost - inner_path->startup_cost) * innerstartsel;
        inner_run_cost = (inner_path->total_cost - inner_path->startup_cost) * (innerendsel - innerstartsel);
    }

    // Store results in workspace for final_cost_mergejoin
    workspace->startup_cost = startup_cost;
    workspace->total_cost = startup_cost + run_cost + inner_run_cost;
    workspace->run_cost = run_cost;
    workspace->inner_run_cost = inner_run_cost;
    workspace->outer_rows = outer_rows;
    workspace->inner_rows = inner_rows;
    workspace->outer_skip_rows = outer_skip_rows;
    workspace->inner_skip_rows = inner_skip_rows;
}
```