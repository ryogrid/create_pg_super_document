# pg_extension_update_paths

## Location
[src/backend/commands/extension.c:2339-2423](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L2339-L2423)

## Overview
Reports the version update paths that exist for a specified extension, providing information about how to upgrade or downgrade between different extension versions.

## Definition

```c
Datum
pg_extension_update_paths(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL SQL-callable function that analyzes an extension's control file and script directory to determine all possible version update paths. It returns a set of rows showing the source version, target version, and the path of intermediate versions needed to get from one version to another. The function uses Dijkstra's shortest path algorithm internally to find the most efficient update sequences.

The function reads the extension's control file and extracts version information from available update scripts in the extension's script directory. For each pair of versions, it attempts to find the shortest update path and returns the results as a table with three columns: source version, target version, and the path string showing intermediate steps.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (Name): The name of the extension to analyze for update paths

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME: Extracts extension name from function arguments
  - check_valid_extension_name: Validates extension name format
  - InitMaterializedSRF: Initializes set-returning function infrastructure
  - read_extension_control_file: Reads extension control file
  - get_ext_ver_list: Extracts version information from extension scripts
  - find_update_path: Finds shortest path between two extension versions
  - tuplestore_putvalues: Stores result rows in tuple store
- Called from:
  - SQL queries via system function calls (typically invoked as SELECT * FROM pg_extension_update_paths('extension_name'))

## Notes and Other Information
- This is a set-returning function (SRF) that can be called from SQL
- The function validates the extension name before performing any filesystem operations
- Returns NULL for the path column when no update path exists between two versions
- The path string format shows versions connected by '--' (e.g., '1.0--1.1--1.2')
- The function examines all possible version pairs, making it potentially expensive for extensions with many versions
- Located in src/backend/commands/extension.c:2339-2423