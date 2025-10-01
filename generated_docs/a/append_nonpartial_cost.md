# append_nonpartial_cost

## Location
[src/backend/optimizer/path/costsize.c:2155-2230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L2155-L2230)

## Overview
Estimates the cost of the non-partial paths in a Parallel Append operation by simulating work distribution among parallel workers.

## Definition

```c
static Cost
append_nonpartial_cost(List *subpaths, int numpaths, int parallel_workers)
```
## Detailed Description
This function calculates the execution cost for non-partial paths in a parallel append operation. It models how work is distributed among parallel workers by:

1. Creating an array representing the cost accumulation for each worker (size is minimum of parallel_workers and numpaths)
2. Initially assigning the first few paths (up to array length) to different workers, one path per worker
3. For remaining paths, always assigning each to the worker with the currently lowest accumulated cost
4. Returning the highest cost among all workers (representing the total execution time)

The algorithm assumes subpaths are sorted in decreasing order of cost, which ensures optimal load balancing since expensive paths are assigned first to separate workers.

## Parameters / Member Variables
- : List of Path objects representing the subpaths to be processed
- : Number of non-partial paths to consider from the beginning of subpaths list  
- : Number of parallel workers available for execution

## Dependencies
- Functions called/Symbols referenced:
  - Cost (type)
  - for_each_cell
  - [palloc](../p/palloc.md)
  - Min
  - [Path](../P/Path.md) (struct)
- Called from (representative examples):
  - [cost_append](../c/cost_append.md)

## Notes and Other Information
- Returns 0 if numpaths is 0 (no non-partial paths to process)
- Uses a greedy load-balancing algorithm that assigns work to the least-loaded worker
- The returned cost represents the completion time of the slowest worker (bottleneck)
- Critical for accurately estimating parallel append performance
- Only considers non-partial paths; partial paths are handled separately in parallel operations
- Static function used internally by the cost estimation system

## Simplified Source

```c
static Cost
append_nonpartial_cost(List *subpaths, int numpaths, int parallel_workers)
{
    Cost *worker_costs;
    int num_workers;
    int path_index;
    int min_worker;

    if (numpaths == 0)
        return 0;

    // Create array to track cost for each worker
    num_workers = Min(parallel_workers, numpaths);
    worker_costs = (Cost *) palloc(sizeof(Cost) * num_workers);

    // Assign first paths to different workers (one per worker)
    path_index = 0;
    foreach(cell, subpaths)
    {
        Path *subpath = (Path *) lfirst(cell);

        if (path_index == num_workers)
            break;
        worker_costs[path_index++] = subpath->total_cost;
    }

    // Find initial minimum cost worker (last one due to decreasing cost order)
    min_worker = num_workers - 1;

    // Assign remaining paths to worker with minimum cost
    for_each_cell(remaining_cell, subpaths, cell)
    {
        Path *subpath = (Path *) lfirst(remaining_cell);

        if (path_index++ == numpaths)
            break;

        // Add cost to least loaded worker
        worker_costs[min_worker] += subpath->total_cost;

        // Find new minimum cost worker
        min_worker = 0;
        for (int i = 1; i < num_workers; i++)
        {
            if (worker_costs[i] < worker_costs[min_worker])
                min_worker = i;
        }
    }

    // Return maximum cost (bottleneck worker determines total time)
    Cost max_cost = worker_costs[0];
    for (int i = 1; i < num_workers; i++)
    {
        if (worker_costs[i] > max_cost)
            max_cost = worker_costs[i];
    }

    return max_cost;
}
```