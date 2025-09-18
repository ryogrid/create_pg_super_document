# extend_pattern_info_array

## Location
src/bin/pg_amcheck/pg_amcheck.c: 1334 - 1355

## Overview
A utility function that dynamically extends a PatternInfoArray to accommodate one additional PatternInfo entry, returning a pointer to the newly allocated and initialized entry.

## Definition


## Detailed Description
The  function manages dynamic memory allocation for pattern information arrays used in pg_amcheck's pattern matching system. It increases the array size by one element, reallocates memory to accommodate the larger array, and initializes the new entry to zero.

This function is a core utility for building up collections of database, schema, and relation patterns during command-line argument processing. It ensures that the PatternInfoArray can grow as needed to store user-specified patterns for objects to be checked.

The function handles memory management safely by using pg_realloc, which will terminate the program on allocation failure, ensuring that the caller always receives a valid pointer to the new entry.

## Parameters / Member Variables
- : Pointer to the PatternInfoArray structure to be extended

## Dependencies
- Functions called/Symbols referenced:
  - [PatternInfoArray](../P/PatternInfoArray.md) (struct type)
  - [PatternInfo](../P/PatternInfo.md) (struct type)
  - pg_realloc
  - memset
- Called from:
  - [append_database_pattern](../a/append_database_pattern.md) (at src/bin/pg_amcheck/pg_amcheck.c:1360)
  - [append_schema_pattern](../a/append_schema_pattern.md) (at src/bin/pg_amcheck/pg_amcheck.c:1391)
  - [append_relation_pattern_helper](../a/append_relation_pattern_helper.md) (at src/bin/pg_amcheck/pg_amcheck.c:1435)

## Notes and Other Information
- This is a static function, only accessible within pg_amcheck.c
- Uses pg_realloc which will exit the program on memory allocation failure
- Always initializes the new PatternInfo entry to zero using memset
- Increments the array length before reallocation
- Returns a pointer to the newly added entry for immediate use by the caller
- Part of the pattern management system that handles user-specified inclusion/exclusion patterns for database objects
- The function assumes the PatternInfoArray structure has been properly initialized before calling