# read_restore_filters

## Location
[src/bin/pg_dump/pg_restore.c:550-640](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_restore.c#L550-L640)

## Overview
The read_restore_filters function parses a filter file containing object identifier patterns and populates the appropriate include and exclude lists in the RestoreOptions structure for use during database restoration operations.

## Definition

```c
static void
read_restore_filters(const char *filename, RestoreOptions *opts)
```
## Detailed Description
This function implements the core logic for processing filter files in pg_restore, allowing users to selectively include or exclude specific database objects during restoration. The function reads filter commands from a file (or STDIN if filename is "-") and processes them line by line.

The function supports:
- Include filters for functions, indexes, schemas, tables, and triggers
- Exclude filters for schemas only
- Comprehensive error checking for unsupported filter combinations
- Dynamic memory management for filter patterns

For include operations, the function sets appropriate selection flags in the RestoreOptions structure and appends object names to the corresponding string lists. For exclude operations, currently only schema exclusion is supported. The function validates filter types and reports errors for unsupported combinations.

## Parameters / Member Variables
- `*filename`: Path to the filter file to read, or "-" to read from STDIN
- `*opts`: Pointer to RestoreOptions structure that will be populated with filter information
## Dependencies
- Functions called/Symbols referenced:
  - [filter_init](../f/filter_init.md)
  - [filter_read_item](../f/filter_read_item.md)
  - [filter_free](../f/filter_free.md)
  - [simple_string_list_append](../s/simple_string_list_append.md)
  - [pg_log_filter_error](../p/pg_log_filter_error.md)
  - [filter_object_type_name](../f/filter_object_type_name.md)
  - [exit_nicely](../e/exit_nicely.md)
  - free
- Types used:
  - [RestoreOptions](../R/RestoreOptions.md)
  - FilterStateData
  - FilterCommandType
  - [FilterObjectType](../F/FilterObjectType.md)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_dump/pg_restore.c:295)

## Notes and Other Information
- This function is declared as static, limiting its scope to the pg_restore.c source file
- The function enforces strict rules about which object types can be included or excluded
- Include filters are supported for: functions, indexes, schemas, tables, and triggers
- Exclude filters are currently only supported for schemas
- The function performs comprehensive error checking and exits with error code 1 for invalid filter specifications
- Memory allocated for object names is properly freed after processing
- Located in src/bin/pg_dump/pg_restore.c:550-640