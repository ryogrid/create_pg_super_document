# generate_gather_paths

## Location
src/backend/optimizer/path/allpaths.c: 3052 - 3121

## Overview
Generates parallel access paths for a relation by creating Gather and GatherMerge paths on top of existing partial paths, enabling parallel query execution.

## Definition
```c
void
generate_gather_paths(PlannerInfo *root, RelOptInfo *rel, bool override_rows)
```

## Detailed Description
This function is responsible for creating parallel execution paths by wrapping existing partial paths with Gather or GatherMerge nodes. It must be called only after all partial paths for the relation have been created to avoid reference issues when paths are deleted by add_partial_path.

The function creates two types of parallel paths:
1. A simple Gather path using the cheapest partial path (always unsorted output)
2. GatherMerge paths that preserve ordering for each partial path with useful pathkeys

The function handles row count estimation by scaling the partial path rows by the number of parallel workers. It also supports overriding the relation's row count estimate, which is particularly useful for partially-grouped or partially-distinct operations.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global query planning information
- `rel`: RelOptInfo structure representing the relation for which parallel paths are being generated
- `override_rows`: Boolean flag indicating whether to override the relation's row count estimate

## Dependencies
- Functions called/Symbols referenced:
  - create_gather_path
  - add_path
  - create_gather_merge_path
  - GatherMergePath (type)
- Called from (representative examples):
  - generate_useful_gather_paths

## Notes and Other Information
- Must be called after all partial paths are created to avoid dangling references
- Gather paths always produce unsorted output, so only the cheapest partial path is used for simple Gather
- GatherMerge paths preserve ordering and are created for each partial path with non-NIL pathkeys
- Row count scaling accounts for parallel workers: subpath_rows × parallel_workers
- The override_rows parameter is essential for operations like partial grouping where the base relation estimate is inadequate
- Requires existing partial_pathlist to be non-empty to generate any paths