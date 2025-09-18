# pqAddTuple

## Location
[src/interfaces/libpq/fe-exec.c:993-1059](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L993-L1059)

## Overview
pqAddTuple is a private utility function that adds a row pointer to a PGresult structure, automatically growing the tuple array when necessary to accommodate new tuples.

## Definition


## Detailed Description
This function manages the dynamic growth of the tuple array within a PGresult structure. When adding a new tuple would exceed the current array capacity, it automatically reallocates memory to double the array size (or sets it to 128 for the initial allocation). The function implements several safety checks including overflow protection for both row count limits (INT_MAX) and memory size limits (SIZE_MAX on 32-bit platforms).

The function uses realloc() for memory expansion, with special handling for the initial allocation case where res->tuples is NULL. It updates the memory accounting in res->memorySize and maintains the integrity of the tuple array structure.

## Parameters / Member Variables
- : Pointer to the PGresult structure that will receive the new tuple
- : Pointer to the PGresAttValue array representing the tuple to be added
- : Double pointer for returning error messages; set to NULL to use default "out of memory" message

## Dependencies
- Functions called/Symbols referenced:
  - [libpq_gettext](../l/libpq_gettext.md) (for error message localization)
  - malloc (for initial array allocation)
  - realloc (for array expansion)
- Types used:
  - PGresAttValue (tuple attribute value type)
- Constants used:
  - INT_MAX (maximum row count limit)
  - SIZE_MAX (memory size limit on 32-bit platforms)
- Called from:
  - [PQsetvalue](../P/PQsetvalue.md)
  - [pqRowProcessor](pqRowProcessor.md)

## Notes and Other Information
- The function is marked as static, indicating it's for internal libpq use only
- Implements exponential growth strategy: doubles array size when expansion is needed, starting with 128 entries
- Includes platform-specific overflow checks for 32-bit systems where SIZE_MAX might be exceeded before INT_MAX
- Handles the special case where realloc() might not behave like malloc() on some older C libraries (like SunOS 4.1.x)
- Updates memory accounting (res->memorySize) to track total allocated memory
- Returns false on allocation failure or overflow conditions, true on success
- The tuple array positions beyond res->ntups contain garbage data, not necessarily NULL
- Enforces a hard limit of INT_MAX tuples per result set since row numbers use integers