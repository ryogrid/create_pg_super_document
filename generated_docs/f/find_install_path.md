# find_install_path

## Location
[src/backend/commands/extension.c:1403-1457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L1403-L1457)

## Overview
Finds the optimal installation path to reach a target extension version that is not directly installable by evaluating all possible starting points from installable versions.

## Definition
```c
static ExtensionVersionInfo *find_install_path(List *evi_list, ExtensionVersionInfo *evi_target, List **best_path)
```

## Detailed Description
This function solves the problem of installing an extension version that doesn't have a direct installation script. It systematically evaluates all directly installable versions as potential starting points, calculates the shortest update path from each to the target, and selects the optimal route.

The selection criteria prioritize:
1. Shortest update path length (fewest version transitions)
2. Lexicographically smaller starting version name (for deterministic tie-breaking)

The function uses the reject_indirect optimization when calling find_update_path, avoiding paths through other installable versions since it will evaluate those separately as potential starting points.

## Parameters / Member Variables
- `evi_list`: Complete list of ExtensionVersionInfo vertices representing available versions
- `evi_target`: Target version to reach (assumed to be non-installable)
- `best_path`: Output parameter receiving the optimal update path sequence

## Dependencies
- Functions called/Symbols referenced:
  - [find_update_path](find_update_path.md)
  - [ExtensionVersionInfo](../E/ExtensionVersionInfo.md) (struct type)  
  - lfirst (list iteration)
  - [list_length](../l/list_length.md) (path comparison)
  - strcmp (tie-breaking)
- Called from (representative examples):
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md)
  - [get_available_versions_for_extension](../g/get_available_versions_for_extension.md)

## Notes and Other Information
- Static function only used within extension.c module
- Returns NULL if target is unreachable from any installable version
- Handles the edge case where target is already installable (returns target with empty path)
- Uses reject_indirect=true optimization to avoid redundant path exploration
- Deterministic tie-breaking ensures consistent behavior across different environments
- Essential for PostgreSQL's extension installation when target version lacks direct install script
- best_path output parameter set to NIL initially and updated with optimal path on success
- Each candidate path is calculated independently with full reinitialization

## Simplified Source

```c
static ExtensionVersionInfo *find_install_path(List *evi_list,
                                              ExtensionVersionInfo *evi_target,
                                              List **best_path) {
    ExtensionVersionInfo *evi_start = NULL;
    *best_path = NIL;

    // Quick return: if target is already installable, no path needed
    if (evi_target->installable) {
        return evi_target;
    }

    // Try each installable version as a potential starting point
    foreach(lc, evi_list) {
        ExtensionVersionInfo *evi1 = (ExtensionVersionInfo *) lfirst(lc);

        if (!evi1->installable)
            continue;

        // Find shortest path from this installable version to target
        List *path = find_update_path(evi_list, evi1, evi_target, true, true);
        if (path == NIL)
            continue;

        // Check if this is the best path so far
        // Prefer: 1) shorter paths, 2) lexicographically smaller start version
        if (evi_start == NULL ||
            list_length(path) < list_length(*best_path) ||
            (list_length(path) == list_length(*best_path) &&
             strcmp(evi_start->name, evi1->name) < 0)) {
            evi_start = evi1;
            *best_path = path;
        }
    }

    return evi_start;  // NULL if no path found
}
```