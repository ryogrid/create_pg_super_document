# ExtensionVersionInfo

## Location
[src/backend/commands/extension.c:98-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L98-L107)

## Overview
ExtensionVersionInfo is an internal data structure that represents version information and update path relationships for PostgreSQL extensions, used primarily in extension update path calculations and Dijkstra's shortest path algorithm.

## Definition
```c
typedef struct ExtensionVersionInfo
{
    char                   *name;           /* name of the starting version */
    List                   *reachable;      /* List of ExtensionVersionInfo's */
    bool                    installable;    /* does this version have an install script? */
    /* working state for Dijkstra's algorithm: */
    bool                    distance_known; /* is distance from start known yet? */
    int                     distance;       /* current worst-case distance estimate */
    struct ExtensionVersionInfo *previous;  /* current best predecessor */
} ExtensionVersionInfo;
```

## Detailed Description
The ExtensionVersionInfo structure serves as a vertex in a directed graph representing extension version relationships and update paths. It is used to implement Dijkstra's shortest path algorithm for finding optimal extension update sequences. Each instance represents a specific version of an extension and maintains information about reachable versions, installation capabilities, and algorithm state. The structure enables PostgreSQL to calculate the shortest update path between extension versions, ensuring efficient and safe extension upgrades.

## Parameters / Member Variables
- `name`: The version string identifier for this specific extension version
- `reachable`: List of ExtensionVersionInfo structures representing versions directly reachable from this version
- `installable`: Boolean flag indicating whether this version has a direct installation script (as opposed to being only reachable via updates)
- `distance_known`: Working state flag for Dijkstra's algorithm indicating whether the shortest distance from the starting version has been determined
- `distance`: Current distance estimate from the starting version in the shortest path calculation
- `previous`: Pointer to the previous ExtensionVersionInfo in the current best path, used for path reconstruction

## Dependencies
- Functions called/Symbols referenced:
  - [ExtensionVersionInfo](ExtensionVersionInfo.md) (self-referential for previous field)
- Called from (representative examples):
  - [get_ext_ver_info](../g/get_ext_ver_info.md)
  - [get_ext_ver_list](../g/get_ext_ver_list.md)
  - [identify_update_path](../i/identify_update_path.md)
  - [find_update_path](../f/find_update_path.md)
  - [find_install_path](../f/find_install_path.md)
  - [get_nearest_unprocessed_vertex](../g/get_nearest_unprocessed_vertex.md)
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md)

## Notes and Other Information
This structure is essential for extension version management and implements a graph-based approach to finding optimal update paths. The Dijkstra algorithm state fields (distance_known, distance, previous) are temporary working variables used during path-finding calculations. The structure supports both direct installation (installable=true) and update-only versions, enabling flexible extension deployment strategies. The reachable list creates a directed graph where edges represent valid update paths between versions.