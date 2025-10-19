# byte_increment

## Location
[src/backend/utils/adt/like_support.c:1523-1572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L1523-L1572)

## Overview
Increments a single byte value for bytea data type pattern matching, used to create the next lexicographically greater string.

## Definition
```c
static bool byte_increment(unsigned char *ptr, int len)
```

## Detailed Description
This function performs a simple byte increment operation specifically designed for bytea (binary data) pattern matching. Unlike text data which requires complex multibyte character handling, bytea data can be processed byte-by-byte. The function increments the byte value pointed to by `ptr` by 1, unless it's already at the maximum value (255), in which case it returns false to indicate overflow.

This function is used as part of PostgreSQL's pattern matching optimization where it helps construct the lexicographically next string for range queries in LIKE operations.

## Parameters / Member Variables
- `ptr`: Pointer to the unsigned char (byte) to be incremented
- `len`: Length parameter (not used in this simple implementation but maintained for interface consistency)

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic arithmetic operations)
- Called from (representative examples):
  - [make_greater_string](../m/make_greater_string.md)

## Notes and Other Information
- This is a static function within like_support.c, used internally for bytea pattern processing
- The function is designed for bytea data which doesn't have multibyte character encoding concerns
- Returns true if increment was successful, false if the byte was already at maximum value (255)
- The `len` parameter is not used in this implementation but is kept for consistency with similar increment functions
- Part of PostgreSQL's LIKE pattern optimization infrastructure

## Simplified Source

```c
// Simplified version of byte_increment
static bool byte_increment(unsigned char *ptr, int len) {
    // Check if byte is already at maximum (255)
    if (*ptr >= 255)
        return false;

    // Increment the byte value
    (*ptr)++;
    return true;
}
```