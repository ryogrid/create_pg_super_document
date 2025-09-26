# GatherPath

## Location
[src/include/nodes/pathnodes.h:2041-2047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2041-L2047)

## Overview
GatherPath represents a path node for parallel query execution that runs multiple copies of a plan in parallel and collects the results from worker processes.

## Definition
```c
typedef struct GatherPath
{
    Path        path;
    Path       *subpath;        /* path for each worker */
    bool        single_copy;    /* dont execute path more than once */
    int         num_workers;    /* number of workers sought to help */
} GatherPath;
```

## Detailed Description
GatherPath is a specialized path node used in PostgreSQL query planning to represent parallel execution strategies. It coordinates the execution of a subplan across multiple worker processes, with the parallel leader potentially also participating in execution unless the single_copy flag is set. This path type is fundamental to PostgreSQL parallel query processing, allowing the optimizer to consider parallelization as a viable execution strategy for appropriate queries.

The GatherPath serves as a coordination point where results from multiple parallel workers are collected and combined. It inherits from the base Path structure, providing all standard path node functionality while adding parallel-specific attributes.

## Parameters / Member Variables
- `path`: Base Path structure containing standard path information (cost, ordering, etc.)
- `subpath`: Pointer to the Path that will be executed by each worker process
- `single_copy`: Boolean flag indicating whether the path should be executed only once (when true, only workers execute, not the leader)
- `num_workers`: The number of worker processes requested to help execute this path

## Dependencies
- Functions called/Symbols referenced:
  - [Path](../P/Path.md) (base structure)

- Called from (representative examples):
  - [cost_gather](../c/cost_gather.md) (calculates execution cost)
  - [create_gather_plan](../c/create_gather_plan.md) (converts path to execution plan)
  - [create_gather_path](../c/create_gather_path.md) (creates new GatherPath instances)
  - [apply_projection_to_path](../a/apply_projection_to_path.md) (applies projections to parallel paths)

## Notes and Other Information
- [GatherPath](GatherPath.md) is primarily used for parallel sequential scans and other parallelizable operations
- The single_copy flag is important for operations where having the leader participate would cause correctness issues
- Cost estimation for GatherPath involves considering parallel startup costs, worker coordination overhead, and potential load balancing issues
- The actual number of workers allocated may be less than num_workers based on system resources and configuration