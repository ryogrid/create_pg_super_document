# create_gather_plan

## Location
src/backend/optimizer/plan/createplan.c: 1920 - 1957

## Overview
Creates a Gather plan node that coordinates parallel execution by collecting results from multiple worker processes.

## Definition


## Detailed Description
The  function creates a Gather plan node that serves as the coordination point for parallel query execution. This node runs in the leader process and collects results from multiple worker processes executing the subplan in parallel.

Key aspects of the implementation:
- **Projection pushdown**: The function pushes projection work down to the child node using CP_EXACT_TLIST flag, ensuring that projection work is parallelized across worker processes
- **System column handling**: By pushing projection down, it ensures no system columns appear in the result, which is necessary because tuple queues use MinimalTuple representation that cannot contain system columns  
- **Parallel coordination**: The Gather node manages communication with worker processes through tuple queues
- **Resource management**: Assigns a special execution parameter for coordinating parallel execution and enables parallel mode globally

## Parameters / Member Variables
- : PlannerInfo containing planner state and global execution context
- : GatherPath specifying the parallel execution strategy, including number of workers and whether single-copy mode should be used

## Dependencies
- Functions called/Symbols referenced:
  - create_plan_recurse (with CP_EXACT_TLIST flag)
  - build_path_tlist
  - make_gather
  - assign_special_exec_param
  - copy_generic_path_info
- Called from (representative examples):
  - create_plan_recurse

## Notes and Other Information
- The function automatically enables parallel mode by setting 
- Uses CP_EXACT_TLIST flag when creating the subplan to ensure projection is pushed down to worker processes
- The special execution parameter assigned is used for coordinating parallel execution between leader and workers
- Single-copy mode allows certain operations to be performed by only one worker to avoid duplicate work
- Gather nodes cannot preserve ordering of their input - for ordered parallel execution, GatherMerge should be used instead