# find_update_path

## Location
[src/backend/commands/extension.c:1310-1402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L1310-L1402)

## Overview
Implements Dijkstra's shortest path algorithm to find the optimal sequence of version transitions between two extension versions in the dependency graph.

## Definition
```c
static List *find_update_path(List *evi_list, ExtensionVersionInfo *evi_start, ExtensionVersionInfo *evi_target, bool reject_indirect, bool reinitialize)
```

## Detailed Description
This function is the core implementation of Dijkstra's algorithm for finding shortest paths in PostgreSQL's extension version dependency graph. It processes vertices in order of distance from the start vertex, updating distances to reachable neighbors and tracking the shortest path.

Key features include:
- Full Dijkstra implementation with distance tracking and previous vertex pointers
- Optional rejection of indirect paths through installable versions to optimize performance
- Deterministic tie-breaking using lexicographic comparison of version names
- Efficient early termination when target is reached
- Path reconstruction by following previous vertex pointers backward

The algorithm ensures optimal update paths while providing flexibility for different use cases through the reject_indirect and reinitialize parameters.

## Parameters / Member Variables
- `evi_list`: Complete list of ExtensionVersionInfo vertices representing the version graph
- `evi_start`: Starting vertex for path calculation
- `evi_target`: Target vertex to reach
- `reject_indirect`: If true, ignore paths through installable versions for optimization
- `reinitialize`: If true, reset all transient fields; false assumes clean initialization

## Dependencies
- Functions called/Symbols referenced:
  - [get_nearest_unprocessed_vertex](../g/get_nearest_unprocessed_vertex.md)
  - [ExtensionVersionInfo](../E/ExtensionVersionInfo.md) (struct type)
  - lfirst (list iteration)
  - [lcons](../l/lcons.md) (list construction)
  - strcmp (string comparison for tie-breaking)
  - Assert (debugging assertions)
- Called from (representative examples):
  - [identify_update_path](../i/identify_update_path.md)
  - [find_install_path](find_install_path.md)
  - [pg_extension_update_paths](../p/pg_extension_update_paths.md)

## Notes and Other Information
- Classic Dijkstra's algorithm implementation adapted for extension version graphs
- Uses unit edge weights (each version transition costs 1)
- Deterministic tie-breaking ensures consistent results regardless of directory traversal order
- reject_indirect optimization useful when caller will try all installable starting points
- Returns path as list of version names excluding the starting version
- Early termination when target found or all remaining vertices unreachable
- Static function only used within extension.c module
- Distance field uses INT_MAX to represent infinity/unreachable state

## Simplified Source

```c
static List *find_update_path(List *evi_list,
                             ExtensionVersionInfo *evi_start,
                             ExtensionVersionInfo *evi_target,
                             bool reject_indirect,
                             bool reinitialize) {
    Assert(evi_start != evi_target);
    Assert(!(reject_indirect && evi_target->installable));

    // Initialize all vertices if needed
    if (reinitialize) {
        ListCell *lc;
        foreach(lc, evi_list) {
            ExtensionVersionInfo *evi = (ExtensionVersionInfo *) lfirst(lc);
            evi->distance_known = false;
            evi->distance = INT_MAX;
            evi->previous = NULL;
        }
    }

    // Start Dijkstra's algorithm
    evi_start->distance = 0;

    // Process vertices in order of distance
    ExtensionVersionInfo *evi;
    while ((evi = get_nearest_unprocessed_vertex(evi_list)) != NULL) {
        if (evi->distance == INT_MAX)
            break;  // All remaining vertices unreachable

        evi->distance_known = true;

        if (evi == evi_target)
            break;  // Found shortest path to target

        // Update distances to reachable neighbors
        ListCell *lc;
        foreach(lc, evi->reachable) {
            ExtensionVersionInfo *evi2 = (ExtensionVersionInfo *) lfirst(lc);

            // Skip installable versions if requested
            if (reject_indirect && evi2->installable)
                continue;

            int newdist = evi->distance + 1;

            if (newdist < evi2->distance) {
                // Found shorter path
                evi2->distance = newdist;
                evi2->previous = evi;
            } else if (newdist == evi2->distance &&
                      evi2->previous != NULL &&
                      strcmp(evi->name, evi2->previous->name) < 0) {
                // Break ties deterministically by version name
                evi2->previous = evi;
            }
        }
    }

    // Check if target is reachable
    if (!evi_target->distance_known)
        return NIL;

    // Build path by following previous pointers backward
    List *result = NIL;
    for (evi = evi_target; evi != evi_start; evi = evi->previous)
        result = lcons(evi->name, result);

    return result;
}
```