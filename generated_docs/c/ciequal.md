# ciequal

## Location
[src/timezone/zic.c:3614-3622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L3614-L3622)

## Overview
A case-insensitive string equality comparison function used in PostgreSQL's timezone compilation utilities to compare strings without regard to letter case.

## Definition
```c
static bool ciequal(const char *ap, const char *bp)
```

## Detailed Description
The `ciequal` function performs case-insensitive string comparison by comparing each character of two strings after converting them to lowercase using the `lowerit` function. The function iterates through both strings character by character, converting each to lowercase before comparison. If all characters match (including the null terminator), the function returns `true`. If any character differs, it immediately returns `false`.

This function is essential for timezone parsing where string matching needs to be case-insensitive, allowing timezone abbreviations and names to be recognized regardless of their case.

## Parameters / Member Variables
- `ap`: Pointer to the first null-terminated string to compare
- `bp`: Pointer to the second null-terminated string to compare

## Dependencies
- Functions called/Symbols referenced:
  - [lowerit](../l/lowerit.md) (src/timezone/zic.c:3616)
- Called from (representative examples):
  - [byword](../b/byword.md) (src/timezone/zic.c:3680)

## Notes and Other Information
- Returns `true` if both strings are equal when compared case-insensitively, `false` otherwise
- Uses the locale-independent `lowerit` function to ensure consistent behavior across different system configurations
- Part of PostgreSQL's timezone compilation utilities (zic)
- Commonly used for matching timezone abbreviations and keywords where case should not matter
- The function handles null-terminated strings and stops comparison at the first differing character or when both strings reach their null terminators

## Simplified Source

```c
static bool ciequal(const char *ap, const char *bp)
{
    // Compare strings character by character, case-insensitively
    while (lowerit(*ap) == lowerit(*bp++)) {
        if (*ap++ == '\0')
            return true;  // Reached end of both strings
    }
    return false;  // Found a difference
}
```