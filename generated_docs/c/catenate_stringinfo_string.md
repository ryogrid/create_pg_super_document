# catenate_stringinfo_string

## Location
src/backend/utils/adt/json.c: 1200 - 1214

## Overview
A static helper function that combines a StringInfo buffer with an additional string and returns the result as a PostgreSQL text datum.

## Definition
```c
static text *catenate_stringinfo_string(StringInfo buffer, const char *addon)
```

## Detailed Description
This function serves as a specialized utility for PostgreSQL aggregate finalization functions that need to append additional text to accumulated StringInfo buffers while converting the result to a proper PostgreSQL text type. It implements a custom version of `cstring_to_text_with_len` that efficiently concatenates the StringInfo buffer contents with a trailing string in a single memory allocation.

The function is designed specifically for aggregate final functions, which are not allowed to modify the aggregate state. Instead of modifying the original StringInfo buffer, this function creates a new text datum containing both the buffer contents and the additional string. The implementation handles proper PostgreSQL variable-length type formatting including VARHDRSZ sizing and VARDATA placement.

## Parameters / Member Variables
- `buffer`: StringInfo buffer containing the accumulated string data
- `addon`: Null-terminated string to append to the buffer contents

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - SET_VARSIZE (PostgreSQL macro for variable-length type sizing)
  - VARDATA (PostgreSQL macro for variable-length data access)
  - memcpy (standard C library)
- Called from (representative examples):
  - [json_agg_finalfn](../j/json_agg_finalfn.md)
  - [json_object_agg_finalfn](../j/json_object_agg_finalfn.md)

## Notes and Other Information
- This is a static internal function within the json.c module
- Specifically designed for aggregate finalization where state modification is prohibited
- Implements efficient single-allocation concatenation rather than using separate string operations
- Returns a properly formatted PostgreSQL text datum with correct VARHDRSZ header
- The function is essential for JSON aggregate functions that need to append closing brackets or other terminators
- Located in src/backend/utils/adt/json.c:1200-1214