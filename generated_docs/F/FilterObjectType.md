# FilterObjectType

## Location
[src/bin/pg_dump/filter.h:61-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/filter.h#L61-L71)

## Overview
An enumeration that defines the types of database objects that can be specified in filter files used by PostgreSQL's pg_dump, pg_dumpall, and pg_restore utilities for selective inclusion/exclusion operations.

## Definition

```c
typedef enum
{
	FILTER_OBJECT_TYPE_NONE,
	FILTER_OBJECT_TYPE_TABLE_DATA,
	FILTER_OBJECT_TYPE_TABLE_DATA_AND_CHILDREN,
	FILTER_OBJECT_TYPE_DATABASE,
	FILTER_OBJECT_TYPE_EXTENSION,
	FILTER_OBJECT_TYPE_FOREIGN_DATA,
	FILTER_OBJECT_TYPE_FUNCTION,
	FILTER_OBJECT_TYPE_INDEX,
	FILTER_OBJECT_TYPE_SCHEMA,
	FILTER_OBJECT_TYPE_TABLE,
	FILTER_OBJECT_TYPE_TABLE_AND_CHILDREN,
	FILTER_OBJECT_TYPE_TRIGGER,
} FilterObjectType;
```
## Detailed Description
FilterObjectType is an enumeration used by PostgreSQL's backup and restore utilities to categorize different types of database objects that can be filtered during dump and restore operations. Each enum value corresponds to a specific object type that can be included or excluded through filter files. The enum is primarily used in conjunction with FilterCommandType to specify whether objects of a particular type should be included or excluded from backup/restore operations.

## Parameters / Member Variables
- : Represents comments, empty lines, or unspecified object types
- : Represents table data only (without schema)
- : Represents table data including inherited tables
- : Represents database objects
- : Represents PostgreSQL extensions
- : Represents foreign data wrappers and foreign tables
- : Represents functions and procedures
- : Represents database indexes
- : Represents database schemas
- : Represents tables (schema only)
- : Represents tables including inherited tables
- : Represents database triggers

## Dependencies
- Functions called/Symbols referenced:
  - [filter_object_type_name](../f/filter_object_type_name.md) (converts enum values to string representations)
- Called from (representative examples):
  - [get_object_type](../g/get_object_type.md) (src/bin/pg_dump/filter.c:123)
  - [filter_read_item](../f/filter_read_item.md) (src/bin/pg_dump/filter.c:396)
  - [read_dump_filters](../r/read_dump_filters.md) (src/bin/pg_dump/pg_dump.c:19063)
  - [read_dumpall_filters](../r/read_dumpall_filters.md) (src/bin/pg_dump/pg_dumpall.c:2048)
  - [read_restore_filters](../r/read_restore_filters.md) (src/bin/pg_dump/pg_restore.c:555)

## Notes and Other Information
- Defined in src/bin/pg_dump/filter.h:47-61
- Used across pg_dump, pg_dumpall, and pg_restore utilities for consistent object type identification
- The enum values map to specific string keywords in filter files (e.g., "table_data", "schema", "function")
- String conversion is handled by the filter_object_type_name() function for error messaging
- Part of PostgreSQL's selective backup/restore filtering system introduced for fine-grained control over dump operations