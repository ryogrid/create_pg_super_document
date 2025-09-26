# MaterialPath

## Location
src/include/nodes/pathnodes.h: 1981 - 1985

## Overview
MaterialPath represents use of a Material plan node for caching the output of its subpath, used when the subpath is expensive and needs to be scanned repeatedly or when mark/restore ability is required.

## Definition
```c
typedef struct MaterialPath
{
	Path		path;
	Path	   *subpath;
} MaterialPath;
```

## Detailed Description
MaterialPath is a path node that represents a Material plan node, which acts as a caching layer for its subpath. The Material node stores the complete output of its subpath in memory or temporary storage, allowing it to be rescanned efficiently without re-executing the underlying plan. This is particularly useful in two scenarios:

1. When the subpath is expensive to execute and needs to be scanned multiple times (e.g., in nested loop joins where the inner relation is complex)
2. When mark/restore functionality is needed but the subpath doesnt natively support it (mark/restore allows rewinding to a previously marked position in the result set)

The Material node essentially converts a non-rescannable or expensive-to-rescan plan into one that can be rescanned cheaply after the initial execution.

## Parameters / Member Variables
- `path`: Base Path structure containing common path information like cost estimates, row count, and pathkeys
- `subpath`: Pointer to the underlying Path node whose output will be materialized and cached

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - create_plan_recurse
  - create_material_plan
  - create_material_path
  - reparameterize_path

## Notes and Other Information
- Material nodes trade memory usage for execution time by caching results
- Pathkeys are inherited from the subpath since materialization preserves ordering
- Cost calculation includes the overhead of storing and retrieving cached data
- Cannot be parallel-aware itself but inherits parallel safety from its subpath
- Commonly inserted by the planner for merge joins, hash joins, and other operations requiring mark/restore
- The cost_material() function accounts for the additional I/O and memory costs
- Parameter information is inherited from the subpath since materialization doesnt change parameterization