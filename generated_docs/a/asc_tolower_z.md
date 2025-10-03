# asc_tolower_z

## Location
[src/backend/utils/adt/formatting.c:2253-2258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L2253-L2258)

## Overview
A convenience wrapper function that converts a null-terminated string to lowercase using ASCII-only character transformation.

## Definition

```c
static char *
asc_tolower_z(const char *buff)
```
## Detailed Description
This function is a simplified wrapper around the  function that automatically determines the string length using . It provides ASCII-only lowercase conversion for null-terminated strings, eliminating the need for the caller to specify the buffer length explicitly. The function is static to the formatting.c module and is primarily used within PostgreSQL's numeric formatting operations.

## Parameters / Member Variables
- `*buff`: A null-terminated input string to be converted to lowercase
## Dependencies
- Functions called/Symbols referenced:
  - [asc_tolower](asc_tolower.md)
  - strlen
- Called from (representative examples):
  - [NUM_processor](../N/NUM_processor.md)

## Notes and Other Information
- This is a static function local to src/backend/utils/adt/formatting.c
- The function assumes the input string is null-terminated, unlike its parent function  which accepts a byte count
- Returns a palloc'd string that must be freed by the caller
- Used specifically in numeric formatting operations within PostgreSQL's formatting system
- The 'z' suffix indicates this variant works with null-terminated (zero-terminated) strings