# find_update_path

## Location
src/backend/commands/extension.c: 1310 - 1402

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
  - get_nearest_unprocessed_vertex
  - ExtensionVersionInfo (struct type)
  - lfirst (list iteration)
  - lcons (list construction)
  - strcmp (string comparison for tie-breaking)
  - Assert (debugging assertions)
- Called from (representative examples):
  - identify_update_path
  - find_install_path
  - pg_extension_update_paths

## Notes and Other Information
- Classic Dijkstra's algorithm implementation adapted for extension version graphs
- Uses unit edge weights (each version transition costs 1)
- Deterministic tie-breaking ensures consistent results regardless of directory traversal order
- reject_indirect optimization useful when caller will try all installable starting points
- Returns path as list of version names excluding the starting version
- Early termination when target found or all remaining vertices unreachable
- Static function only used within extension.c module
- Distance field uses INT_MAX to represent infinity/unreachable state