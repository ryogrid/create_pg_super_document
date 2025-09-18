# PatternInfoArray

## Location
src/bin/pg_amcheck/pg_amcheck.c: 47 - 51

## Overview
PatternInfoArray is a dynamic array structure used in PostgreSQL's pg_amcheck utility to manage collections of PatternInfo objects for pattern matching operations.

## Definition
```c
typedef struct PatternInfoArray
{
    PatternInfo *data;
    size_t      len;
} PatternInfoArray;
```

## Detailed Description
The PatternInfoArray structure implements a simple dynamic array container specifically designed to hold PatternInfo objects. It follows a common C pattern for managing collections, maintaining a pointer to the data array and tracking the current length. This structure is used throughout pg_amcheck to organize and process multiple pattern matching rules that can be applied to database objects. The array supports operations like extending with new patterns and iterating through existing patterns for matching operations.

## Parameters / Member Variables
- `data`: Pointer to the array of PatternInfo structures
- `len`: Current number of elements in the array

## Dependencies
- Functions called/Symbols referenced:
  - PatternInfo (as array element type)
- Called from (representative examples):
  - AmcheckOptions (contains PatternInfoArray members)
  - extend_pattern_info_array
  - append_database_pattern
  - append_schema_pattern
  - append_relation_pattern_helper
  - append_heap_pattern
  - append_btree_pattern

## Notes and Other Information
- Defined in src/bin/pg_amcheck/pg_amcheck.c:47-51
- Used as a container for managing multiple PatternInfo objects in pg_amcheck
- Implements a simple dynamic array pattern common in C programming
- The structure does not include capacity information, suggesting it may use realloc-based growth
- Used in AmcheckOptions to store different types of patterns (include and exclude patterns)
- Supports various pattern operations through dedicated append and extend functions