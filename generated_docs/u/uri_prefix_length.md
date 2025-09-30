# uri_prefix_length

## Location
[src/bin/psql/common.c:2231-2254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L2231-L2254)

## Overview
This function checks if a connection string starts with a valid PostgreSQL URI prefix and returns the length of the prefix if found.

## Definition

```c
static int
uri_prefix_length(const char *connstr)
```
## Detailed Description
The  function is a utility that validates and measures the URI prefix portion of PostgreSQL connection strings. It recognizes two valid URI designators:
1.  - the standard PostgreSQL URI prefix
2.  - the shorter alternative URI prefix

The function compares the beginning of the input connection string against these two valid prefixes using . If a match is found, it returns the length of the matching prefix (excluding the null terminator). If no valid prefix is found, it returns 0.

This function is noted as a duplicate of a similar function in libpq, indicating that psql needs its own copy for local connection string processing without depending on libpq's internal functions.

## Parameters / Member Variables
- : Pointer to the connection string to be examined for URI prefix

## Dependencies
- Functions called/Symbols referenced:
  - strncmp (standard C library function for string comparison)
- Called from (representative examples):
  - [recognized_connection_string](../r/recognized_connection_string.md) (in both psql and libpq)
  - [parse_connection_string](../p/parse_connection_string.md) (in libpq)
  - [conninfo_uri_parse_options](../c/conninfo_uri_parse_options.md) (in libpq)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file
- The function is explicitly noted as a duplicate of libpq's equivalent function
- Used for validating PostgreSQL URI connection strings before parsing
- Supports both the full  and shortened  URI schemes
- Returns the exact byte length of the prefix, which can be used to skip over the prefix when parsing the rest of the URI
- Essential for distinguishing between URI-style and keyword=value style connection strings
- The function only validates the prefix, not the entire URI syntax

## Simplified Source

```c
static int uri_prefix_length(const char *connstr) {
    // Check for standard "postgresql://" prefix
    if (strncmp(connstr, "postgresql://", 13) == 0)
        return 13;

    // Check for short "postgres://" prefix
    if (strncmp(connstr, "postgres://", 11) == 0)
        return 11;

    // No valid URI prefix found
    return 0;
}
```