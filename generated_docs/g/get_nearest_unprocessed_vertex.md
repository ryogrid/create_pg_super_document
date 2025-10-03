# get_nearest_unprocessed_vertex

## Location
[src/backend/commands/extension.c:1176-1203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L1176-L1203)

## Overview
Locates the nearest unprocessed ExtensionVersionInfo vertex during extension version dependency resolution using Dijkstra's algorithm.

## Definition

```c
static ExtensionVersionInfo *
get_nearest_unprocessed_vertex(List *evi_list)
```
## Detailed Description
This function implements part of Dijkstra's shortest path algorithm for finding extension upgrade paths. It searches through a list of ExtensionVersionInfo vertices to find the one with the smallest distance value among those that haven't been processed yet (distance_known = false). The algorithm is O(N^2) complexity as noted in the comments - a priority queue implementation would be more efficient but is not currently needed.

The function is a key component in PostgreSQL's extension version resolution system, helping determine the optimal upgrade path between extension versions by finding the next closest vertex to process in the dependency graph.

## Parameters / Member Variables
- `*evi_list`: List of ExtensionVersionInfo structures representing extension version vertices in the dependency graph
## Dependencies
- Functions called/Symbols referenced:
  - [ExtensionVersionInfo](../E/ExtensionVersionInfo.md) (struct type)
  - lfirst (list iteration macro)
- Called from (representative examples):  
  - [find_update_path](../f/find_update_path.md)

## Notes and Other Information
- Part of Dijkstra's algorithm implementation for extension version path finding
- Current O(N^2) implementation could be optimized with priority queue but deemed unnecessary for typical use cases
- Only considers vertices where distance_known is false (unprocessed vertices)
- Returns NULL if no unprocessed vertices remain
- Static function - only used within extension.c module

## Simplified Source

```c
static ExtensionVersionInfo *
get_nearest_unprocessed_vertex(List *evi_list)
{
    ExtensionVersionInfo *closest = NULL;
    ListCell *lc;

    // Find the unprocessed vertex with smallest distance
    foreach(lc, evi_list) {
        ExtensionVersionInfo *current = (ExtensionVersionInfo *) lfirst(lc);

        // Skip already processed vertices
        if (current->distance_known)
            continue;

        // Update closest if this vertex is nearer
        if (closest == NULL || closest->distance > current->distance)
            closest = current;
    }

    return closest;
}
```