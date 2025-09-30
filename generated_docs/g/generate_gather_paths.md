# generate_gather_paths

## Location
[src/backend/optimizer/path/allpaths.c:3052-3121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L3052-L3121)

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
  - [create_gather_path](../c/create_gather_path.md)
  - [add_path](../a/add_path.md)
  - [create_gather_merge_path](../c/create_gather_merge_path.md)
  - [GatherMergePath](../G/GatherMergePath.md) (type)
- Called from (representative examples):
  - [generate_useful_gather_paths](generate_useful_gather_paths.md)

## Notes and Other Information
- Must be called after all partial paths are created to avoid dangling references
- [Gather](../G/Gather.md) paths always produce unsorted output, so only the cheapest partial path is used for simple Gather
- [GatherMerge](../G/GatherMerge.md) paths preserve ordering and are created for each partial path with non-NIL pathkeys
- Row count scaling accounts for parallel workers: subpath_rows × parallel_workers
- The override_rows parameter is essential for operations like partial grouping where the base relation estimate is inadequate
- Requires existing partial_pathlist to be non-empty to generate any paths

## Simplified Source

```c
void
generate_gather_paths(PlannerInfo *root, RelOptInfo *rel, bool override_rows)
{
    Path *cheapest_partial_path;
    Path *simple_gather_path;
    double rows;
    double *rowsp = NULL;

    // Exit early if no partial paths available
    if (rel->partial_pathlist == NIL)
        return;

    // Set up row count override if requested
    if (override_rows)
        rowsp = &rows;

    // Create simple Gather path using cheapest partial path
    // (Gather output is always unsorted)
    cheapest_partial_path = linitial(rel->partial_pathlist);
    rows = cheapest_partial_path->rows * cheapest_partial_path->parallel_workers;

    simple_gather_path = (Path *)
        create_gather_path(root, rel, cheapest_partial_path, rel->reltarget,
                          NULL, rowsp);
    add_path(rel, simple_gather_path);

    // Create GatherMerge paths for each ordered partial path
    foreach(lc, rel->partial_pathlist)
    {
        Path *subpath = (Path *) lfirst(lc);
        GatherMergePath *path;

        // Skip unordered paths (already handled by simple Gather)
        if (subpath->pathkeys == NIL)
            continue;

        // Create order-preserving GatherMerge path
        rows = subpath->rows * subpath->parallel_workers;
        path = create_gather_merge_path(root, rel, subpath, rel->reltarget,
                                       subpath->pathkeys, NULL, rowsp);
        add_path(rel, &path->path);
    }
}
```