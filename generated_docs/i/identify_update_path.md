# identify_update_path

## Location
[src/backend/commands/extension.c:1267-1309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L1267-L1309)

## Overview
Determines the sequence of version transitions required to update an extension from one version to another using the shortest path algorithm.

## Definition
```c
static List *identify_update_path(ExtensionControlFile *control, const char *oldVersion, const char *newVersion)
```

## Detailed Description
This function orchestrates the process of finding an update path between two extension versions. It first builds the complete version dependency graph by scanning available update scripts, then uses Dijkstra's shortest path algorithm to find the optimal sequence of version transitions.

The function serves as a high-level interface that combines graph construction and pathfinding. If no valid path exists between the specified versions, it raises an error with a descriptive message. The returned list contains the sequence of intermediate versions to transition through (excluding the starting version).

## Parameters / Member Variables
- `control`: ExtensionControlFile containing extension metadata and configuration
- `oldVersion`: Starting version string for the update path
- `newVersion`: Target version string for the update path

## Dependencies
- Functions called/Symbols referenced:
  - [get_ext_ver_list](../g/get_ext_ver_list.md)
  - [get_ext_ver_info](../g/get_ext_ver_info.md)
  - [find_update_path](../f/find_update_path.md)
  - ereport (for error handling)
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - ExecAlterExtensionStmt

## Notes and Other Information
- Static function only used within extension.c module
- Returns NIL if no path exists, triggering an ERROR
- The returned path excludes the starting version but includes all intermediate and final versions
- Part of PostgreSQL's ALTER EXTENSION ... UPDATE TO command implementation
- Uses shortest path algorithm to minimize the number of update script executions required
- Error message includes extension name and both version strings for debugging