# filter_object_type_name

## Location
src/bin/pg_dump/filter.c: 83 - 122

## Overview
Converts FilterObjectType enum values to their corresponding human-readable string representations, primarily for error message formatting.

## Definition
```c
const char *filter_object_type_name(FilterObjectType fot)
```

## Detailed Description
This function provides a mapping from FilterObjectType enumeration values to descriptive string names. It uses a switch statement to handle all possible filter object types supported by the pg_dump utilities. The function is mainly used for generating user-friendly error messages when filter processing encounters issues. The function includes an unreachable code path to handle unexpected enum values.

## Parameters / Member Variables
- `fot`: FilterObjectType enum value to convert to string representation

## Dependencies
- Functions called/Symbols referenced:
  - pg_unreachable
  - [FilterObjectType](../F/FilterObjectType.md) enum constants:
    - FILTER_OBJECT_TYPE_NONE
    - FILTER_OBJECT_TYPE_TABLE_DATA
    - FILTER_OBJECT_TYPE_TABLE_DATA_AND_CHILDREN
    - FILTER_OBJECT_TYPE_DATABASE
    - FILTER_OBJECT_TYPE_EXTENSION
    - FILTER_OBJECT_TYPE_FOREIGN_DATA
    - FILTER_OBJECT_TYPE_FUNCTION
    - FILTER_OBJECT_TYPE_INDEX
    - FILTER_OBJECT_TYPE_SCHEMA
    - FILTER_OBJECT_TYPE_TABLE
    - FILTER_OBJECT_TYPE_TABLE_AND_CHILDREN
    - FILTER_OBJECT_TYPE_TRIGGER
- Called from (representative examples):
  - [read_dump_filters](../r/read_dump_filters.md) (in pg_dump.c)
  - [read_dumpall_filters](../r/read_dumpall_filters.md) (in pg_dumpall.c)
  - [read_restore_filters](../r/read_restore_filters.md) (in pg_restore.c)

## Notes and Other Information
- Returns constant string literals that do not need to be freed
- Covers all FilterObjectType enum values defined in the system
- Uses pg_unreachable() to handle impossible cases, which helps with compiler optimizations
- The strings returned are user-facing and should be suitable for error messages
- The function is exhaustive and handles all known filter object types used in pg_dump operations