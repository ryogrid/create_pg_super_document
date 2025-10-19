# strip_quotes

## Location
[src/bin/psql/stringutils.c:240-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/stringutils.c#L240-L291)

## Overview
A utility function that removes quotes from a string in-place, handling both leading/trailing quotes and embedded escaped quotes according to specified quote and escape characters.

## Definition

```c
void
strip_quotes(char *source, char quote, char escape, int encoding)
```
## Detailed Description
The strip_quotes function modifies a string in-place to remove quote characters and process escape sequences. It removes leading and trailing occurrences of the specified quote character, converts doubled quote characters to single quotes (a common SQL escaping convention), and processes escape sequences where the escape character removes special significance from the following character. The function is designed to work with multi-byte character encodings and is commonly used to clean up tokens that have been parsed with quote preservation.

The function operates destructively on the input string, overwriting it with the cleaned version. This approach is memory-efficient as it doesn't require additional allocation.

## Parameters / Member Variables
- `*source`: Input string to be modified in-place (must not be NULL)
- `quote`: The quote character to be stripped and processed (must not be '\0')
- `escape`: Character that removes special significance from the next character (0 if none)
- `encoding`: Active character-set encoding for proper multi-byte character handling
## Dependencies
- Functions called/Symbols referenced:
  - [PQmblenBounded](../P/PQmblenBounded.md)
- Called from (representative examples):
  - [strtokx](strtokx.md)
  - [parse_slash_copy](../p/parse_slash_copy.md)

## Notes and Other Information
- The source string is modified in-place, so the original content is lost
- Uses Assert() to validate that source is not NULL and quote is not '\0'
- Properly handles multi-byte characters through PQmblenBounded for encoding support
- Implements the standard SQL convention where doubled quotes represent literal quote characters
- Skip trailing quotes only if they appear at the very end of the string
- Commonly used in conjunction with strtokx when del_quotes=true is specified

## Simplified Source

```c
void strip_quotes(char *source, char quote, char escape, int encoding) {
    char *src, *dst;

    // Initialize source and destination pointers
    src = dst = source;

    // Skip leading quote if present
    if (*src && *src == quote)
        src++;

    // Process each character
    while (*src) {
        char c = *src;

        // Skip trailing quote at end of string
        if (c == quote && src[1] == '\0')
            break;

        // Handle doubled quotes (convert to single)
        else if (c == quote && src[1] == quote)
            src++;

        // Handle escape sequences
        else if (c == escape && src[1] != '\0')
            src++;

        // Copy character(s) handling multi-byte encoding
        int char_len = PQmblenBounded(src, encoding);
        while (char_len--)
            *dst++ = *src++;
    }

    // Null-terminate the result
    *dst = '\0';
}
```