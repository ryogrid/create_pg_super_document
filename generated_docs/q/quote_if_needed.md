# quote_if_needed

## Location
[src/bin/psql/stringutils.c:292-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/stringutils.c#L292-L342)

## Overview
A utility function that determines whether a string needs quoting for safe parsing and returns a properly quoted and escaped version if necessary, serving as the opposite operation to strip_quotes.

## Definition

```c
char *
quote_if_needed(const char *source, const char *entails_quote,
				char quote, char escape, bool force_quote,
				int encoding)
```
## Detailed Description
The quote_if_needed function analyzes a source string to determine if it requires quoting for safe parsing by functions like strtokx() or psql_scan_slash_option(). If the string contains characters that would require special handling during parsing, or if force_quote is true, the function returns a newly allocated string with proper quoting and escaping applied. If no quoting is needed, it returns NULL to indicate the original string can be used as-is.

The function implements proper escaping by doubling quote and escape characters within the string and wrapping the entire result in quote characters. This ensures the resulting string can be safely parsed by PostgreSQL's string parsing functions while preserving the original content.

## Parameters / Member Variables
- `*source`: Input string to analyze and potentially quote (must not be NULL)
- `*entails_quote`: Set of characters whose presence requires the string to be quoted
- `quote`: Quote character to use for wrapping and doubling (must not be '\0')
- `escape`: Escape character to be doubled within the string
- `force_quote`: If true, quote the output even if it doesn't need it based on content analysis
- `encoding`: Active character-set encoding for proper multi-byte character handling
## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md)
  - [PQmblenBounded](../P/PQmblenBounded.md)
- Called from (representative examples):
  - [complete_from_files](../c/complete_from_files.md)
  - [quote_file_name](quote_file_name.md)

## Notes and Other Information
- Returns NULL if no quoting is needed, otherwise returns a malloc'd copy that must be freed by caller
- Should not be used as a substitute for PQescapeStringConn() for SQL string escaping
- Specifically designed for strings that will be parsed by strtokx() or psql_scan_slash_option()
- Allocates excess memory (2 * strlen + 3) to accommodate worst-case escaping scenarios
- Properly handles multi-byte characters through encoding parameter
- The force_quote parameter allows unconditional quoting even when content analysis suggests it's not needed

## Simplified Source

```c
char *quote_if_needed(const char *source, const char *entails_quote,
                     char quote, char escape, bool force_quote,
                     int encoding) {
    const char *src;
    char *ret, *dst;
    bool need_quotes = force_quote;

    // Allocate buffer for worst-case scenario (every char doubled + quotes)
    src = source;
    dst = ret = pg_malloc(2 * strlen(src) + 3);

    // Start with opening quote
    *dst++ = quote;

    // Process each character in source
    while (*src) {
        char c = *src;

        // Double quote characters and mark as needing quotes
        if (c == quote) {
            need_quotes = true;
            *dst++ = quote;
        }
        // Double escape characters and mark as needing quotes
        else if (c == escape) {
            need_quotes = true;
            *dst++ = escape;
        }
        // Check if character requires quoting
        else if (strchr(entails_quote, c)) {
            need_quotes = true;
        }

        // Copy the actual character(s) handling multi-byte encoding
        int char_len = PQmblenBounded(src, encoding);
        while (char_len--)
            *dst++ = *src++;
    }

    // Add closing quote and null terminator
    *dst++ = quote;
    *dst = '\0';

    // Return quoted string only if quoting was needed
    if (!need_quotes) {
        free(ret);
        ret = NULL;
    }

    return ret;
}
```