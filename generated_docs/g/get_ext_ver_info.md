# get_ext_ver_info

## Location
[src/backend/commands/extension.c:1143-1175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L1143-L1175)

## Overview
Finds or creates an ExtensionVersionInfo structure for a specified version name, maintaining a list of version information for extension management.

## Definition
```c
static ExtensionVersionInfo *get_ext_ver_info(const char *versionname, List **evi_list)
```

## Detailed Description
This function manages a collection of ExtensionVersionInfo structures that represent different versions of an extension and their relationships. It implements a simple cache/registry pattern where version information is stored in a list and retrieved by name.

The function serves two purposes:
1. **Lookup**: If a version with the given name already exists in the list, it returns the existing ExtensionVersionInfo
2. **Creation**: If no matching version exists, it creates a new ExtensionVersionInfo with default values

When creating new version information, the function initializes the structure with default values suitable for later use in Dijkstra's algorithm for finding update paths. This includes setting up distance tracking fields and marking the version as not installable initially.

The function uses a linear search through the version list, which the comments acknowledge could become O(N²) for extensions with many versions, though this is noted as acceptable for current use cases.

## Parameters / Member Variables
- `versionname`: The name/identifier of the extension version to find or create
- `evi_list`: Pointer to a List pointer containing ExtensionVersionInfo structures (modified if new version is created)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison for version name lookup)
  - [palloc](../p/palloc.md) (memory allocation for new ExtensionVersionInfo)
  - [pstrdup](../p/pstrdup.md) (duplicate version name string)
  - [lappend](../l/lappend.md) (append new version info to list)
  - lfirst (access list cell contents)
- Called from:
  - [get_ext_ver_list](get_ext_ver_list.md) (multiple calls for building version graph)
  - [identify_update_path](../i/identify_update_path.md) (for path finding)
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md) (during extension installation)

## Notes and Other Information
- This is a static function within the extension.c module
- Uses O(N) search time which could become O(N²) for extensions with many versions
- The comment suggests this could be optimized with a hash table if performance becomes an issue
- Initializes new version info with values suitable for Dijkstra's algorithm (distance tracking)
- The `reachable` field is initialized to NIL (empty list) for storing reachable versions
- The `installable` flag defaults to false and is set elsewhere based on available scripts
- Memory allocated by this function (ExtensionVersionInfo and version name) should be freed by the caller context
- This function is part of PostgreSQL's extension update path calculation system

## Simplified Source

```c
static ExtensionVersionInfo *get_ext_ver_info(const char *versionname,
                                             List **evi_list) {
    ExtensionVersionInfo *evi;

    // Search for existing version info by name
    foreach(lc, *evi_list) {
        evi = (ExtensionVersionInfo *) lfirst(lc);
        if (strcmp(evi->name, versionname) == 0) {
            return evi;  // Found existing version
        }
    }

    // Create new version info if not found
    evi = (ExtensionVersionInfo *) palloc(sizeof(ExtensionVersionInfo));
    evi->name = pstrdup(versionname);
    evi->reachable = NIL;
    evi->installable = false;

    // Initialize for Dijkstra's algorithm
    evi->distance_known = false;
    evi->distance = INT_MAX;
    evi->previous = NULL;

    // Add to list and return
    *evi_list = lappend(*evi_list, evi);
    return evi;
}
```