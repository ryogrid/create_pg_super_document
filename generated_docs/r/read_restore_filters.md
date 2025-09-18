# read_restore_filters

## Location
src/bin/pg_dump/pg_restore.c: 550 - 640

## Overview
The read_restore_filters function parses a filter file containing object identifier patterns and populates the appropriate include and exclude lists in the RestoreOptions structure for use during database restoration operations.

## Definition


## Detailed Description
This function implements the core logic for processing filter files in pg_restore, allowing users to selectively include or exclude specific database objects during restoration. The function reads filter commands from a file (or STDIN if filename is "-") and processes them line by line.

The function supports:
- Include filters for functions, indexes, schemas, tables, and triggers
- Exclude filters for schemas only
- Comprehensive error checking for unsupported filter combinations
- Dynamic memory management for filter patterns

For include operations, the function sets appropriate selection flags in the RestoreOptions structure and appends object names to the corresponding string lists. For exclude operations, currently only schema exclusion is supported. The function validates filter types and reports errors for unsupported combinations.

## Parameters / Member Variables
- : Path to the filter file to read, or "-" to read from STDIN
- : Pointer to RestoreOptions structure that will be populated with filter information

## Dependencies
- Functions called/Symbols referenced:
  - filter_init
  - filter_read_item
  - filter_free
  - simple_string_list_append
  - pg_log_filter_error
  - filter_object_type_name
  - exit_nicely
  - free
- Types used:
  - RestoreOptions
  - FilterStateData
  - FilterCommandType
  - FilterObjectType
- Called from (representative examples):
  - main (in src/bin/pg_dump/pg_restore.c:295)

## Notes and Other Information
- This function is declared as static, limiting its scope to the pg_restore.c source file
- The function enforces strict rules about which object types can be included or excluded
- Include filters are supported for: functions, indexes, schemas, tables, and triggers
- Exclude filters are currently only supported for schemas
- The function performs comprehensive error checking and exits with error code 1 for invalid filter specifications
- Memory allocated for object names is properly freed after processing
- Located in src/bin/pg_dump/pg_restore.c:550-640