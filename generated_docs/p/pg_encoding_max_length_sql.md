# pg_encoding_max_length_sql

## Location
[src/backend/utils/mb/mbutils.c:644-659](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L644-L659)

## Overview
A PostgreSQL SQL function that returns the maximum number of bytes that can be used to represent a single character in the specified character encoding.

## Definition
```c
Datum pg_encoding_max_length_sql(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_encoding_max_length_sql` function provides a SQL-accessible way to query the maximum byte length of a character in a given encoding. Unlike some other encoding-related functions, this one takes a numeric encoding ID rather than an encoding name. It looks up the maximum multibyte character length from PostgreSQL's internal `pg_wchar_table` which contains encoding-specific information.

This function is useful for applications that need to allocate appropriate buffer sizes or understand the storage requirements for different character encodings, particularly when dealing with variable-width encodings like UTF-8.

## Parameters / Member Variables
- `encoding` (INT4): The numeric identifier of the character encoding

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro for extracting INT32 argument)
  - PG_VALID_ENCODING (macro to validate encoding ID)
  - pg_wchar_table (global table containing encoding information)
  - PG_RETURN_INT32 (macro for returning INT32 result)
  - PG_RETURN_NULL (macro for returning NULL result)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- Returns NULL for invalid encoding IDs rather than raising an error
- Uses the `maxmblen` field from the `pg_wchar_table` array
- The encoding parameter is a numeric ID, not a string name
- Part of PostgreSQL's character encoding metadata system
- Located in src/backend/utils/mb/mbutils.c:644-659

## Simplified Source

```c
Datum pg_encoding_max_length_sql(PG_FUNCTION_ARGS) {
    // Get the encoding ID from function arguments
    int encoding = PG_GETARG_INT32(0);

    // Return max character length if encoding is valid, otherwise NULL
    if (PG_VALID_ENCODING(encoding))
        PG_RETURN_INT32(pg_wchar_table[encoding].maxmblen);
    else
        PG_RETURN_NULL();
}
```