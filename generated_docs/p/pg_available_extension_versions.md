# pg_available_extension_versions

## Location
[src/backend/commands/extension.c:2088-2145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L2088-L2145)

## Overview
This function provides a set-returning function (SRF) that lists all available versions for PostgreSQL extensions by scanning extension control files and their associated version scripts.

## Definition


## Detailed Description
The pg_available_extension_versions function scans the extension control directory and, for each available extension, discovers all available versions by examining installation scripts in the extension's script directory. Unlike pg_available_extensions which returns one row per extension, this function returns one row per available version of each extension.

The function works by:
1. Reading all primary control files from the extension control directory
2. For each extension found, calling get_available_versions_for_extension to scan for version-specific installation scripts
3. Building a result set containing detailed information about each available version

This function provides the backend implementation for the pg_available_extension_versions system view, which adds information about which versions are currently installed in the database.

## Parameters / Member Variables
This function uses the PostgreSQL function call convention and doesn't take explicit parameters beyond the standard .

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md)
  - [get_extension_control_directory](../g/get_extension_control_directory.md)
  - AllocateDir
  - ReadDir
  - [is_extension_control_filename](../i/is_extension_control_filename.md)
  - [read_extension_control_file](../r/read_extension_control_file.md)
  - [get_available_versions_for_extension](../g/get_available_versions_for_extension.md)
  - FreeDir
- Called from (representative examples):
  - No direct references found (typically called via SQL function interface)

## Notes and Other Information
- This function is designed to be called from SQL as a set-returning function
- It provides more detailed version information compared to pg_available_extensions
- The function delegates the actual version discovery logic to get_available_versions_for_extension
- Like pg_available_extensions, it gracefully handles missing control directories
- Only processes primary control files, ignoring auxiliary control files with "--" in their names
- The result set structure and columns are determined by get_available_versions_for_extension
- This function is the foundation for PostgreSQL's extension version management system