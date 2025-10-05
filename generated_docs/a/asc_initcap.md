# asc_initcap

## Location
[src/backend/utils/adt/formatting.c:2204-2234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L2204-L2234)

## Overview
A utility function that converts ASCII characters in a string to initial capital format (first letter of each word capitalized) for PostgreSQL formatting operations.

## Definition
char *asc_initcap(const char *buff, size_t nbytes)

## Detailed Description
The asc_initcap function implements ASCII-only initial capitalization functionality for PostgreSQL's formatting system. It processes a character buffer and creates a new string where the first letter of each word is converted to uppercase and subsequent letters are converted to lowercase. Words are identified by sequences of alphanumeric characters (A-Z, a-z, 0-9), with non-alphanumeric characters serving as word boundaries.

The function uses a state-tracking approach with the wasalnum variable to determine whether the current character is at the beginning of a word or within a word. This allows for proper capitalization logic where only the first character of each alphanumeric sequence is uppercased.

## Parameters / Member Variables
- : Input character buffer to convert (can be NULL)
- : Number of bytes to process from the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - [pnstrdup](../p/pnstrdup.md)
  - [pg_ascii_tolower](../p/pg_ascii_tolower.md)
  - [pg_ascii_toupper](../p/pg_ascii_toupper.md)
- Called from (representative examples):
  - [str_initcap](../s/str_initcap.md)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Only processes ASCII characters, ignoring locale-specific rules
- Uses explicit ASCII range checks rather than isalnum() for reliability
- Treats digits (0-9) as part of alphanumeric sequences for word boundary detection
- Designed for consistent behavior across different locales in PostgreSQL's formatting system

## Simplified Source

```c
char *asc_initcap(const char *buff, size_t nbytes) {
    char *result;
    int wasalnum = false;

    if (!buff) return NULL;

    // Copy input string
    result = pnstrdup(buff, nbytes);

    // Process each character
    for (char *p = result; *p; p++) {
        char c;

        // Apply case conversion based on position in word
        if (wasalnum) {
            *p = c = pg_ascii_tolower((unsigned char) *p);
        } else {
            *p = c = pg_ascii_toupper((unsigned char) *p);
        }

        // Check if character is alphanumeric (ASCII only)
        wasalnum = ((c >= 'A' && c <= 'Z') ||
                   (c >= 'a' && c <= 'z') ||
                   (c >= '0' && c <= '9'));
    }

    return result;
}
```