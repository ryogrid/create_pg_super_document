# read_dump_filters

## Location
[src/bin/pg_dump/pg_dump.c:19058-19159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L19058-L19159)

## Overview
Parses filter files to configure include/exclude patterns for PostgreSQL pg_dump operations, determining which database objects should be dumped or excluded from the dump.

## Definition

```c
static void
read_dump_filters(const char *filename, DumpOptions *dopt)
```
## Detailed Description
The  function reads and processes filter configuration from a specified file (or STDIN if filename is "-") to control which database objects are included or excluded during a pg_dump operation. The function parses filter commands that specify object types (tables, schemas, extensions, etc.) and object names or patterns, then populates the appropriate global include/exclude pattern lists.

The function supports two types of filter commands:
- **Include filters**: Add objects matching the pattern to be dumped (for extensions, foreign data, schemas, tables)
- **Exclude filters**: Remove objects matching the pattern from the dump (for extensions, table data, schemas, tables)

Some object types like databases, functions, indexes, and triggers have restrictions on which filter types can be applied to them. The function validates these restrictions and terminates with an error if invalid filter combinations are encountered.

When include filters are applied for schemas or tables, the  flag in DumpOptions is set to false, indicating selective dumping rather than dumping all objects.

## Parameters / Member Variables
- `*filename`: Path to the filter file to read, or "-" to read from STDIN
- `*dopt`: DumpOptions structure that gets updated with  flag based on filter rules
## Dependencies
- Functions called/Symbols referenced:
  - [filter_init](../f/filter_init.md)
  - [filter_read_item](../f/filter_read_item.md)
  - [filter_free](../f/filter_free.md)
  - [simple_string_list_append](../s/simple_string_list_append.md)
  - [pg_log_filter_error](../p/pg_log_filter_error.md)
  - [filter_object_type_name](../f/filter_object_type_name.md)
  - [exit_nicely](../e/exit_nicely.md)
  - FilterStateData (type)
  - FilterCommandType (type)
  - [FilterObjectType](../F/FilterObjectType.md) (type)
  - Various FILTER_* constants
- Called from (representative examples):
  - [main](../m/main.md)
  - fmtQualifiedDumpable

## Notes and Other Information
- This is a static function within pg_dump.c used during command-line option processing
- The function populates global string lists for different object types and operations:
  - extension_include_patterns, extension_exclude_patterns
  - schema_include_patterns, schema_exclude_patterns  
  - table_include_patterns, table_exclude_patterns
  - tabledata_exclude_patterns
  - foreign_servers_include_patterns
  - Pattern lists for "_and_children" variants that include dependent objects
- Invalid filter combinations result in immediate program termination via exit_nicely(1)
- Memory management is handled properly with objname being freed after processing each item
- The function enforces pg_dump's filtering policy where certain object types can only use specific filter commands (include vs exclude)