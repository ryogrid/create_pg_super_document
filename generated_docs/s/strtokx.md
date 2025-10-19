# strtokx

## Location
[src/bin/psql/stringutils.c:52-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/stringutils.c#L52-L239)

## Overview
A sophisticated string tokenization function that serves as a replacement for the standard C strtok() function, providing advanced features like quote handling, escape characters, and PostgreSQL-specific string parsing capabilities.

## Definition

```c
char *
strtokx(const char *s,
		const char *whitespace,
		const char *delim,
		const char *quote,
		char escape,
		bool e_strings,
		bool del_quotes,
		int encoding)
```
## Detailed Description
The strtokx function is a "poor man's flex" implementation that splits strings into tokens with much more sophistication than the standard strtok() function. It supports multiple types of separators, quote handling with escape sequences, and PostgreSQL-specific features like E-string syntax. The function maintains internal state between calls to continue tokenizing the same string, similar to strtok(), but with enhanced capabilities for parsing complex command-line arguments and SQL-like syntax.

Key features include:
- Support for both whitespace and delimiter-based tokenization
- Quote character handling with escape sequences
- PostgreSQL E-string syntax recognition (E'...' format)
- Optional quote stripping from returned tokens
- Multi-byte character encoding support
- Non-destructive parsing (original string remains unchanged)

## Parameters / Member Variables
- `*s`: String to parse; if NULL, continues parsing the last string from previous call
- `*whitespace`: Set of whitespace characters that separate tokens
- `*delim`: Set of non-whitespace separator characters (can be NULL)
- `*quote`: Set of characters that can quote a token (NULL if none)
- `escape`: Character that can escape quotes (0 if none)
- `e_strings`: If true, treat E'...' syntax as a valid quoted token
- `del_quotes`: If true, strip quotes from returned token; otherwise return as found
- `encoding`: Active character-set encoding for multi-byte character support
## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md)
  - [PQmblenBounded](../P/PQmblenBounded.md)
  - [strip_quotes](strip_quotes.md)
- Called from (representative examples):
  - [parse_slash_copy](../p/parse_slash_copy.md)
  - [complete_from_files](../c/complete_from_files.md)
  - [dequote_file_name](../d/dequote_file_name.md)

## Notes and Other Information
- Uses static internal storage to maintain state between calls
- Allocates 2X the input string size to handle potential delimiter insertions
- Double occurrences of quote characters represent single quotes in the data
- The combination of e_strings=true and del_quotes=true is not currently supported
- Changing whitespace characters between calls on the same string is discouraged as it may cause data loss
- Memory is automatically freed when tokenization is complete or a new string is provided

## Simplified Source

```c
char *strtokx(const char *s, const char *whitespace, const char *delim,
              const char *quote, char escape, bool e_strings,
              bool del_quotes, int encoding) {
    static char *storage = NULL;  // Local copy of user's string
    static char *string = NULL;   // Current position in string

    // Initialize with new string if provided
    if (s) {
        free(storage);
        storage = pg_malloc(2 * strlen(s) + 1);  // Extra space for delimiter nulls
        strcpy(storage, s);
        string = storage;
    }

    if (!storage)
        return NULL;

    // Skip leading whitespace
    string += strspn(string, whitespace);
    char *start = string;

    // Check for end of string
    if (*start == '\0') {
        free(storage);
        storage = NULL;
        string = NULL;
        return NULL;
    }

    // Handle delimiter characters
    if (delim && strchr(delim, *start)) {
        char *p = start + 1;
        if (*p != '\0') {
            if (!strchr(whitespace, *p))
                memmove(p + 1, p, strlen(p) + 1);  // Make room for null
            *p = '\0';
            string = p + 1;
        } else {
            string = p;
        }
        return start;
    }

    // Handle E-string syntax (E'...')
    char *p = start;
    if (e_strings && (*p == 'E' || *p == 'e') && p[1] == '\'') {
        quote = "'";
        escape = '\\';
        p++;
    }

    // Handle quoted tokens
    if (quote && strchr(quote, *p)) {
        char thisquote = *p++;

        // Scan for closing quote, handling escapes
        for (; *p; p += PQmblenBounded(p, encoding)) {
            if (*p == escape && p[1] != '\0')
                p++;  // Skip escaped character
            else if (*p == thisquote && p[1] == thisquote)
                p++;  // Skip doubled quote
            else if (*p == thisquote) {
                p++;  // Found closing quote
                break;
            }
        }

        // Terminate token and update position
        if (*p != '\0') {
            if (!strchr(whitespace, *p))
                memmove(p + 1, p, strlen(p) + 1);
            *p = '\0';
            string = p + 1;
        } else {
            string = p;
        }

        // Strip quotes if requested
        if (del_quotes)
            strip_quotes(start, thisquote, escape, encoding);

        return start;
    }

    // Handle unquoted tokens - scan until whitespace, delimiter, or quote
    unsigned int offset = strcspn(start, whitespace);

    if (delim) {
        unsigned int offset2 = strcspn(start, delim);
        if (offset > offset2)
            offset = offset2;
    }

    if (quote) {
        unsigned int offset2 = strcspn(start, quote);
        if (offset > offset2)
            offset = offset2;
    }

    p = start + offset;

    // Terminate token and update position
    if (*p != '\0') {
        if (!strchr(whitespace, *p))
            memmove(p + 1, p, strlen(p) + 1);
        *p = '\0';
        string = p + 1;
    } else {
        string = p;
    }

    return start;
}
```