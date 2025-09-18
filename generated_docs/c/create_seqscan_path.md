# create_seqscan_path

## Location
src/backend/optimizer/util/pathnode.c: 927 - 951

## Overview
Creates a Path node representing a sequential scan access method for a relation, initializing all necessary fields and calculating the associated costs.

## Definition


## Detailed Description
The  function constructs a Path node specifically for sequential scan operations. A sequential scan reads through all pages of a relation in physical storage order without using any indexes. The function initializes all Path structure fields appropriately for this access method and delegates cost calculation to the  function.

Key characteristics of the created path:
- Sets pathtype to T_SeqScan to identify the access method
- Uses the relation's reltarget as the path target
- Handles parameterization through get_baserel_parampathinfo
- Supports parallel execution when parallel_workers > 0
- Always produces unordered results (pathkeys = NIL)
- Marks parallel safety based on the relation's consider_parallel flag

## Parameters / Member Variables
- : The PlannerInfo structure containing global planning context and information
- : The RelOptInfo structure representing the relation to be scanned
- : Set of outer relations whose parameters are needed by this path (for parameterized paths)
- : Number of parallel workers to use (0 for non-parallel scan)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new Path node)
  - get_baserel_parampathinfo (handles parameterization)
  - cost_seqscan (calculates scan costs)
  - T_SeqScan (pathtype constant)
  - NIL (empty list constant)

- Called from (representative examples):
  - set_plain_rel_pathlist
  - create_plain_partial_paths
  - plan_cluster_use_sort
  - reparameterize_path

## Notes and Other Information
- Sequential scans always produce unordered results, hence pathkeys is set to NIL
- The parallel_aware flag is set based on whether parallel_workers > 0
- Parallel safety is determined by the relation's consider_parallel property
- The function creates a basic Path node (not a specialized subclass like IndexPath)
- Cost calculation is delegated to cost_seqscan which considers factors like table size, selectivity, and parallel execution overhead