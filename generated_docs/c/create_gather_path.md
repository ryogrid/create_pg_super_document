# create_gather_path

## Location
src/backend/optimizer/util/pathnode.c: 1972 - 2015

## Overview
Creates a GatherPath node corresponding to a gather scan operation, which is used in PostgreSQL's parallel query execution to combine results from parallel worker processes into a single stream.

## Definition


## Detailed Description
The create_gather_path function constructs a GatherPath node that represents a gather operation in PostgreSQL's query execution plan. A gather path is responsible for collecting results from parallel worker processes and combining them into a single result stream. The function initializes all necessary fields of the GatherPath structure, including cost calculations and path properties.

Key behaviors include:
- Ensures the subpath is parallel-safe before creating the gather path
- Sets the gather path as not parallel-safe itself (since it's the collection point)
- Handles the special case where no workers are available by creating a single-copy gather
- Uses cost_gather to calculate the execution costs
- Results in an unordered output unless it's a single-copy gather

## Parameters / Member Variables
- : PlannerInfo structure containing global planning information and context
- : RelOptInfo structure representing the relation this path operates on
- : The underlying parallel-safe path that will be executed by workers
- : PathTarget specifying the columns and expressions to be returned
- : Relids indicating which outer relations are required for parameter passing
- : Optional row count estimate to override default estimates (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create GatherPath node)
  - get_baserel_parampathinfo (to get parameter information)
  - cost_gather (to calculate execution costs)
  - PathTarget (target column specification)
  - GatherPath (the path node structure)

- Called from (representative examples):
  - generate_gather_paths (in allpaths.c:3077)
  - generate_union_paths (in prepunion.c:884)

## Notes and Other Information
- The function requires the subpath to be parallel-safe, enforced by an Assert
- When num_workers is 0, it creates a single-copy gather that preserves the subpath's pathkeys
- Normal gather operations result in unordered output (pathkeys = NIL)
- The gather path itself is marked as not parallel-safe and not parallel-aware since it serves as the collection point
- Cost calculation is delegated to the cost_gather function, which considers parallel execution overhead