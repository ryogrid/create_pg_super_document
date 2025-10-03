# DatumGetBool

## Location
[src/include/postgres.h:90-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L90-L101)

## Overview
DatumGetBool is an inline function that extracts a boolean value from a PostgreSQL Datum, treating any nonzero value as true.

## Definition

```c
static inline bool
DatumGetBool(Datum X)
```
## Detailed Description
DatumGetBool is a fundamental type conversion function in PostgreSQL's datum system that converts a Datum value to a boolean. The function implements a simple truth evaluation where any nonzero Datum value is considered true, and only zero is considered false. This follows standard C boolean semantics. The function is implemented as a static inline function for optimal performance, as it's used frequently throughout the PostgreSQL codebase for boolean type operations.

## Parameters / Member Variables
- `X`: A Datum value to be converted to boolean. Any nonzero value will be interpreted as true, while zero will be interpreted as false.
## Dependencies
- Functions called/Symbols referenced:
  - None (simple comparison operation)
- Called from (representative examples):
  - Various boolean-handling functions throughout PostgreSQL
  - Type input/output functions
  - Executor functions dealing with boolean expressions

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h for maximum performance
- The function uses PostgreSQL's standard convention where any nonzero value represents true
- Part of PostgreSQL's datum conversion system that provides type-safe access to stored values
- The implementation is deliberately simple for efficiency, as boolean conversion is a frequent operation