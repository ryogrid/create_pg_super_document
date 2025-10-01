# cost_merge_append

## Location
[src/backend/optimizer/path/costsize.c:2404-2452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L2404-L2452)

## Overview
Calculates the startup and total costs for a MergeAppend node, which merges multiple pre-sorted input streams using a heap-based approach to maintain sorted output.

## Definition

```c
void
cost_merge_append(Path *path, PlannerInfo *root,
				  List *pathkeys, int n_streams,
				  Cost input_startup_cost, Cost input_total_cost,
				  double tuples)
```
## Detailed Description
The cost_merge_append function estimates the cost of executing a MergeAppend node in PostgreSQL's query planner. MergeAppend operations merge several pre-sorted input streams while maintaining sort order by using a heap data structure that holds the next tuple from each stream at any given moment.

The costing model accounts for:
- Initial heap construction requiring approximately N*log2(N) tuple comparisons
- Per-output-tuple heap maintenance requiring about log2(N) comparisons to replace the top entry
- A small per-tuple overhead for the merge operation itself

The algorithm assumes the heap is never spilled to disk since N (number of streams) is typically not very large, making this simpler than sort costing. Each tuple comparison is charged as two operator evaluations.

## Parameters / Member Variables
- : The Path node to store the calculated costs in
- : PlannerInfo structure containing planner context (currently unused)
- : List of sort keys used for the merge operation
- : Number of input streams to be merged
- : Sum of startup costs from all input streams
- : Sum of total costs from all input streams  
- : Total number of tuples across all input streams

## Dependencies
- Functions called/Symbols referenced:
  - LOG2 (logarithm base 2 calculation)
  - Cost (cost data type)
  - APPEND_CPU_COST_MULTIPLIER (constant for append overhead)
- Called from (representative examples):
  - [create_merge_append_path](create_merge_append_path.md) (in pathnode.c:1502)

## Notes and Other Information
- Uses a minimum of 2 streams for logarithm calculation to avoid log(0)
- Charges 2.0 * cpu_operator_cost per tuple comparison
- Adds a small per-tuple overhead using cpu_tuple_cost * APPEND_CPU_COST_MULTIPLIER
- Does not account for the decreasing effective value of N as input streams are exhausted
- Final costs are stored in path->startup_cost and path->total_cost

## Simplified Source

```c
void
cost_merge_append(Path *path, PlannerInfo *root,
                  List *pathkeys, int n_streams,
                  Cost input_startup_cost, Cost input_total_cost,
                  double tuples)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    Cost comparison_cost;
    double N;
    double logN;

    // Avoid log(0) - use minimum of 2 streams
    N = (n_streams < 2) ? 2.0 : (double) n_streams;
    logN = LOG2(N);

    // Cost per tuple comparison (2 operator evaluations)
    comparison_cost = 2.0 * cpu_operator_cost;

    // Initial heap construction: N*log2(N) comparisons
    startup_cost += comparison_cost * N * logN;

    // Per-tuple heap maintenance: log2(N) comparisons per tuple
    run_cost += tuples * comparison_cost * logN;

    // Add small per-tuple overhead for merge operation
    run_cost += cpu_tuple_cost * APPEND_CPU_COST_MULTIPLIER * tuples;

    // Set final path costs
    path->startup_cost = startup_cost + input_startup_cost;
    path->total_cost = startup_cost + run_cost + input_total_cost;
}
```