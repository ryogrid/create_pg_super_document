# filter_read_item

## Location
src/bin/pg_dump/filter.c: 393 - 476

## Overview
Reads and parses a complete filter item (command/type/pattern triplet) from a filter file, handling multi-line patterns and various validation checks.

## Definition
```c
bool filter_read_item(FilterStateData *fstate, char **objname, FilterCommandType *comtype, FilterObjectType *objtype)
```

## Detailed Description
This function parses one filter item from a filter file in the format: `<command> <object_type> <pattern>`. The command can be "include" or "exclude", object types are defined by the FilterObjectType enum, and patterns can be quoted/qualified identifiers with wildcards. The function handles multi-line patterns when object names contain newline characters, skips empty lines and comments (lines starting with #), and performs comprehensive validation with appropriate error messages. Returns true when a filter item is successfully parsed, false at EOF, and exits on errors.

## Parameters / Member Variables
- `fstate`: Pointer to FilterStateData containing file handle, line buffer, and parsing state
- `objname`: Output parameter - pointer to store the allocated object name pattern string
- `comtype`: Output parameter - pointer to store the parsed filter command type (include/exclude)
- `objtype`: Output parameter - pointer to store the parsed object type

## Dependencies
- Functions called/Symbols referenced:
  - pg_get_line_buf
  - [filter_get_keyword](filter_get_keyword.md)
  - is_keyword_str
  - [get_object_type](../g/get_object_type.md)
  - [read_pattern](../r/read_pattern.md)
  - [pg_log_filter_error](../p/pg_log_filter_error.md)
  - [exit_nicely](../e/exit_nicely.md) (via fstate function pointer)
  - initPQExpBuffer
  - isspace (standard C library)
  - ferror (standard C library)
  - FilterStateData, FilterCommandType, FilterObjectType, PQExpBufferData (struct/enum types)
  - FILTER_COMMAND_TYPE_INCLUDE, FILTER_COMMAND_TYPE_EXCLUDE, FILTER_COMMAND_TYPE_NONE, FILTER_OBJECT_TYPE_NONE (enum values)
- Called from (representative examples):
  - [read_dump_filters](../r/read_dump_filters.md) (at src/bin/pg_dump/pg_dump.c:19067)
  - [read_dumpall_filters](../r/read_dumpall_filters.md) (at src/bin/pg_dump/pg_dumpall.c:2052)
  - [read_restore_filters](../r/read_restore_filters.md) (at src/bin/pg_dump/pg_restore.c:559)

## Notes and Other Information
- This is a public function exported from filter.c module
- Central function in pg_dump's filtering system used by pg_dump, pg_dumpall, and pg_restore
- Supports line-based parsing but handles multi-line patterns when object names span lines
- Automatically skips comment lines (starting with #) and empty lines
- Performs strict validation of command keywords ("include" or "exclude") and object types
- Memory for objname pattern is allocated and must be managed by the caller
- Will terminate the program via exit_nicely on any parsing errors or invalid input
- Maintains line number tracking for accurate error reporting
- Returns false only on clean EOF, true for successful parsing or when skipping comments/empty lines