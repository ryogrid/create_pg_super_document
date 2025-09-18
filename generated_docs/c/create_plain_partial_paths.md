# create_plain_partial_paths

## Location
src/backend/optimizer/path/allpaths.c: 794 - 813

## Overview
Creates parallel sequential scan paths for plain base relations by determining the optimal number of parallel workers and generating corresponding partial paths.

## Definition


## Detailed Description
This function generates partial access paths for parallel execution of plain base relations in PostgreSQL's query optimizer. It calculates the optimal number of parallel workers based on the relation's characteristics and system configuration, then creates a parallel sequential scan path if parallel execution is deemed beneficial.

The function first calls  to determine the appropriate number of parallel workers based on the relation's page count and system limits. If the computed number of workers is greater than zero (indicating that parallel execution is worthwhile), it creates a parallel sequential scan path and adds it to the relation's partial pathlist.

The generated partial path represents an unordered parallel sequential scan that can be executed by multiple worker processes, with each worker scanning a portion of the relation's pages. These partial paths can later be combined with gather operations to collect results from all workers.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state and parallel execution configuration
- : RelOptInfo structure representing the relation for which to create parallel paths

## Dependencies
- Functions called/Symbols referenced:
  - [compute_parallel_worker](compute_parallel_worker.md) (calculates optimal number of parallel workers)
  - [add_partial_path](../a/add_partial_path.md) (adds the partial path to relation's pathlist)
  - [create_seqscan_path](create_seqscan_path.md) (creates the parallel sequential scan path)
- Called from:
  - [set_plain_rel_pathlist](../s/set_plain_rel_pathlist.md) (main pathlist generation for plain relations)

## Notes and Other Information
- This is a static function within allpaths.c that specifically handles parallel path generation for plain relations
- The function respects user configuration - if any parallel limits are set to zero, no parallel paths are created
- The parallel worker count calculation considers the relation size, system configuration (max_parallel_workers_per_gather), and other factors
- Generated partial paths are unordered since parallel workers scan different portions of the relation independently
- The partial paths created by this function are later used by gather operations to coordinate parallel execution
- Only sequential scan paths are considered for parallel execution in plain relations - parallel index scans are handled elsewhere
- The function uses -1 as the index pages parameter to compute_parallel_worker, indicating this is for heap scanning rather than index scanning