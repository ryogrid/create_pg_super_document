# make_like_pattern

## Location
[src/bin/psql/tab-complete.c:5953-5998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5953-L5998)

## Overview
Converts a string into a PostgreSQL LIKE pattern by escaping special characters and appending a wildcard for prefix matching.

## Definition
static char *make_like_pattern(const char *word)

## Detailed Description
This function transforms user input into a properly formatted LIKE pattern for PostgreSQL queries. It escapes the special LIKE metacharacters underscore (_) and percent (%) with backslashes, appends a percent sign to create a prefix match pattern, and then passes the result through escape_string() to make it safe for SQL query insertion.

The function also handles multibyte characters correctly by detecting high-bit-set characters and preserving them without modification, using PQmblenBounded() to determine character boundaries. This ensures proper handling of non-ASCII text in various client encodings.

## Parameters / Member Variables
- word: The input string to convert into a LIKE pattern

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md)
  - strlen
  - IS_HIGHBIT_SET
  - [PQmblenBounded](../P/PQmblenBounded.md)
  - [escape_string](../e/escape_string.md)
  - free
- Called from (representative examples):
  - [_complete_from_query](../c/_complete_from_query.md)
  - THING_NO_SHOW completion generator

## Notes and Other Information
The function creates a pattern that matches any string starting with the input word followed by any characters (prefix matching). Multibyte character handling prevents corruption in unsafe client encodings. The returned string is ready for direct insertion into SQL queries and must be freed by the caller. The intermediate buffer is automatically freed before returning.

## Simplified Source

```c
static char *
make_like_pattern(const char *word)
{
    char *result;
    char *buffer = pg_malloc(strlen(word) * 2 + 2);  // Space for escaping + % + null
    char *bptr = buffer;

    // Process each character in the input word
    while (*word) {
        // Escape LIKE special characters _ and %
        if (*word == '_' || *word == '%')
            *bptr++ = '\\';

        if (IS_HIGHBIT_SET(*word)) {
            // Handle multibyte characters safely
            int chlen = PQmblenBounded(word, pset.encoding);

            while (chlen-- > 0)
                *bptr++ = *word++;
        } else {
            // Copy single-byte character
            *bptr++ = *word++;
        }
    }

    // Append wildcard for prefix matching
    *bptr++ = '%';
    *bptr = '\0';

    // Escape the pattern for SQL query insertion
    result = escape_string(buffer);
    free(buffer);
    return result;
}
```