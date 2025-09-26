# namestrcmp

## Location
src/backend/utils/adt/name.c: 247 - 262

## Overview
The  function compares a PostgreSQL  value with a C string using C collation, with proper handling of NULL values.

## Definition

```c
int
namestrcmp(Name name, const char *str)
```
## Detailed Description
This utility function compares a PostgreSQL  structure with a C string using the standard C collation (not locale-aware). The function includes careful NULL handling where NULL values are considered less than any non-NULL value. When both arguments are non-NULL, it delegates to  with  as the maximum comparison length.

The function is primarily designed for equality checks and should be used with caution for ordering operations since it always uses C collation regardless of the database's collation settings. This makes it suitable for internal system comparisons where consistent behavior across locales is required.

## Parameters / Member Variables
- : Pointer to a PostgreSQL  structure (can be NULL)
- : C string to compare against (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to access the character array within a  structure
  - : Standard C library function for bounded string comparison
  - : Constant defining the maximum length of a 
- Called from (representative examples):
  - : Finding attribute numbers by name in COPY operations
  - : Processing field names in COPY FROM operations
  - : Identifying event trigger parameters by name
  - : Looking up tuple attributes by name
  - : Finding field numbers by name in SPI interface
  - : Converting attribute names to numbers

## Notes and Other Information
- Always uses C collation regardless of database collation settings
- NULL handling: NULL < non-NULL, NULL == NULL
- Primarily intended for equality checks rather than ordering operations
- Returns integer comparison result: < 0 (less than), 0 (equal), > 0 (greater than)
- Bounded comparison using  to prevent reading beyond name boundaries
- Located in  at lines 247-262
- Part of PostgreSQL's internal utility functions for  data type manipulation