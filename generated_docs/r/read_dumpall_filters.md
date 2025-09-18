# read_dumpall_filters

## Location
[src/bin/pg_dump/pg_dumpall.c:2043-2090](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L2043-L2090)

## Overview
Reads database filter patterns from a file or STDIN to configure which databases should be excluded from pg_dumpall operations.

## Definition
```c
static void read_dumpall_filters(const char *filename, SimpleStringList *pattern)
```

## Detailed Description
This function parses a filter file to extract database exclusion patterns for pg_dumpall operations. It supports reading from a specified file or from STDIN (when filename is "-"). The function implements strict validation rules:

1. **Only exclusion filters are allowed** - include filters are rejected with an error
2. **Only database object types are supported** - other object types (tables, functions, etc.) are rejected
3. **Database exclusion patterns are collected** - valid database exclusion patterns are added to the provided pattern list

The function uses PostgreSQL's generic filter infrastructure (FilterStateData, filter_init, filter_read_item) to parse the filter file format. It performs comprehensive validation and error reporting, terminating the program if invalid filters are encountered.

## Parameters / Member Variables
- `filename`: Path to filter file, or "-" to read from STDIN
- `pattern`: SimpleStringList to store valid database exclusion patterns

## Dependencies
- Functions called/Symbols referenced:
  - FilterStateData
  - FilterCommandType
  - [FilterObjectType](../F/FilterObjectType.md)
  - [filter_init](../f/filter_init.md)
  - [filter_read_item](../f/filter_read_item.md)
  - FILTER_COMMAND_TYPE_INCLUDE
  - [pg_log_filter_error](../p/pg_log_filter_error.md)
  - [filter_object_type_name](../f/filter_object_type_name.md)
  - [exit_nicely](../e/exit_nicely.md)
  - FILTER_OBJECT_TYPE_* (various constants)
  - [simple_string_list_append](../s/simple_string_list_append.md)
  - [filter_free](../f/filter_free.md)
- Called from (representative examples):
  - [main](../m/main.md) (pg_dumpall)

## Notes and Other Information
- This is a static function within pg_dumpall.c for internal use
- Part of pg_dumpall's filter system for selective database dumping
- Uses PostgreSQL's generic filter parsing infrastructure for consistency
- Implements fail-fast validation - any unsupported filter type causes immediate program termination
- Memory management includes proper cleanup of allocated object names and filter state
- The restriction to database-only filters reflects pg_dumpall's scope as a cluster-wide dump utility
- Filter file format follows the same conventions as other PostgreSQL utilities like pg_dump
- Supports the "-" filename convention for reading filters from STDIN, enabling pipeline usage