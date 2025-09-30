# get_cheapest_parameterized_child_path

## Location
[src/backend/optimizer/path/allpaths.c:1999-2086](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L1999-L2086)

## Overview
Finds the cheapest path for a child relation that has exactly the requested parameterization, potentially reparameterizing existing paths to match the requirement.

## Definition
```c
static Path *get_cheapest_parameterized_child_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer)
```

## Detailed Description
This function implements a sophisticated path selection algorithm for parameterized append relations. It first attempts to find an existing path that already has the exact parameterization needed. If no such path exists, it explores reparameterization of existing paths by pushing down additional join quals to create a path with the required parameterization.

The reparameterization process involves examining all existing paths in the child relation and determining which one becomes cheapest after being reparameterized to match the required outer relation set. This is necessary because different paths may have different costs after reparameterization, and the originally cheapest path may not remain the cheapest after the transformation.

The function implements an optimization by skipping paths that are already more expensive than the current best candidate, since reparameterization can only increase cost, never decrease it.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and optimization context
- `rel`: RelOptInfo structure representing the child relation whose parameterized path is needed
- `required_outer`: Relids (bitmap) specifying the exact set of outer relations that must be available for parameter binding

## Dependencies
- Functions called/Symbols referenced:
  - [get_cheapest_path_for_pathkeys](get_cheapest_path_for_pathkeys.md) (finds cheapest path with specific requirements)
  - PATH_REQ_OUTER (macro to extract required outer relations from a path)
  - [bms_equal](../b/bms_equal.md) (checks if two relation bitmaps are equal)
  - [bms_is_subset](../b/bms_is_subset.md) (checks if one relation bitmap is a subset of another)
  - [compare_path_costs](../c/compare_path_costs.md) (compares costs of two paths for given cost type)
  - [reparameterize_path](../r/reparameterize_path.md) (creates new path with different parameterization)
  - TOTAL_COST (cost comparison type focusing on total execution cost)
- Called from (representative examples):
  - [add_paths_to_append_rel](../a/add_paths_to_append_rel.md) (when building parameterized append paths)

## Notes and Other Information
- Returns NULL if no suitable path can be created with the required parameterization
- The function first tries to find an exact match before attempting reparameterization to avoid unnecessary overhead
- Reparameterization involves pushing down join quals to be evaluated within the child path's scan operation
- The algorithm accounts for the fact that reparameterization costs vary across different base paths
- The function performs cost-based pruning to avoid evaluating obviously inferior alternatives
- Essential for creating parameterized Append paths where all children must have matching parameterization

## Simplified Source

```c
static Path *
get_cheapest_parameterized_child_path(PlannerInfo *root, RelOptInfo *rel,
                                     Relids required_outer)
{
    Path *cheapest;

    // First, look for existing path with no more than needed parameterization
    cheapest = get_cheapest_path_for_pathkeys(rel->pathlist,
                                             NIL,
                                             required_outer,
                                             TOTAL_COST,
                                             false);

    // If we found exact match, return it
    if (bms_equal(PATH_REQ_OUTER(cheapest), required_outer))
        return cheapest;

    // Need to reparameterize - search all paths for best candidate
    cheapest = NULL;
    foreach(lc, rel->pathlist)
    {
        Path *path = (Path *) lfirst(lc);

        // Skip if path needs more parameterization than we can provide
        if (!bms_is_subset(PATH_REQ_OUTER(path), required_outer))
            continue;

        // Skip if already more expensive than current best
        // (reparameterization only increases cost)
        if (cheapest != NULL &&
            compare_path_costs(cheapest, path, TOTAL_COST) <= 0)
            continue;

        // Reparameterize if needed
        if (!bms_equal(PATH_REQ_OUTER(path), required_outer))
        {
            path = reparameterize_path(root, path, required_outer, 1.0);
            if (path == NULL)
                continue;  // Reparameterization failed

            // Recheck cost after reparameterization
            if (cheapest != NULL &&
                compare_path_costs(cheapest, path, TOTAL_COST) <= 0)
                continue;
        }

        // Update best path
        cheapest = path;
    }

    return cheapest;
}
```