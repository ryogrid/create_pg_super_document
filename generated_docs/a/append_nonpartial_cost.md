# append_nonpartial_cost

## Location
src/backend/optimizer/path/costsize.c: 2155 - 2230

## Overview
Estimates the cost of the non-partial paths in a Parallel Append operation by simulating work distribution among parallel workers.

## Definition


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
  - palloc
  - Min
  - Path (struct)
- Called from (representative examples):
  - cost_append

## Notes and Other Information
- Returns 0 if numpaths is 0 (no non-partial paths to process)
- Uses a greedy load-balancing algorithm that assigns work to the least-loaded worker
- The returned cost represents the completion time of the slowest worker (bottleneck)
- Critical for accurately estimating parallel append performance
- Only considers non-partial paths; partial paths are handled separately in parallel operations
- Static function used internally by the cost estimation system