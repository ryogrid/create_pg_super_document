# match_prosrc_to_query

## Location
[src/backend/catalog/pg_proc.c:1069-1126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_proc.c#L1069-L1126)

## Overview
Locates a function body string literal within the original CREATE FUNCTION or DO command text and maps character positions between them.

## Definition
static int match_prosrc_to_query(const char *prosrc, const char *queryText, int cursorpos)

## Detailed Description
This function attempts to find the string literal containing the function body within the original command text. It supports both dollar-quoted literals ($tag$...$tag$) and single-quoted literals ('...'). The function scans through the query text looking for matches and performs position mapping to convert a character index within the function body to the corresponding character index within the original command.

The function uses a simple scanning approach rather than full parsing, which could potentially be fooled in complex scenarios, but works reliably for typical cases. To ensure accuracy, it fails if multiple potential matches are found.

For dollar-quoted literals, position mapping is straightforward since there are no embedded escape characters. For single-quoted literals, it delegates to match_prosrc_to_literal() to handle escape sequences and quote doubling.

## Parameters / Member Variables
- `prosrc`: The function source code string to locate
- `queryText`: The original CREATE FUNCTION or DO command text  
- `cursorpos`: Character position within the prosrc string to map

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mbstrlen_with_len](../p/pg_mbstrlen_with_len.md)
  - [match_prosrc_to_literal](match_prosrc_to_literal.md)
- Called from (representative examples):
  - [function_parse_error_transpose](../f/function_parse_error_transpose.md)

## Notes and Other Information
- Returns the mapped character position in the original query, or 0 if unsuccessful
- Handles multibyte characters correctly using pg_mbstrlen_with_len()
- Fails if multiple matches are found to avoid ambiguity
- Uses simple string scanning rather than full SQL parsing for performance
- Supports both $$ and '' literal syntax commonly used for function bodies
- Character positions are counted in characters, not bytes, for proper Unicode handling

## Simplified Source

```c
static int match_prosrc_to_query(const char *prosrc, const char *queryText, int cursorpos) {
    int prosrclen = strlen(prosrc);
    int querylen = strlen(queryText);
    int matchpos = 0;
    int curpos;
    int newcursorpos;

    // Scan through query text looking for function body matches
    for (curpos = 0; curpos < querylen - prosrclen; curpos++) {

        // Check for dollar-quoted literal: $prosrc$
        if (queryText[curpos] == '$' &&
            strncmp(prosrc, &queryText[curpos + 1], prosrclen) == 0 &&
            queryText[curpos + 1 + prosrclen] == '$') {

            if (matchpos) return 0;  // Multiple matches found, fail

            // Calculate position in multibyte-aware manner
            matchpos = pg_mbstrlen_with_len(queryText, curpos + 1) + cursorpos;
        }

        // Check for single-quoted literal: 'prosrc'
        else if (queryText[curpos] == '\'' &&
                 match_prosrc_to_literal(prosrc, &queryText[curpos + 1],
                                       cursorpos, &newcursorpos)) {

            if (matchpos) return 0;  // Multiple matches found, fail

            // Use adjusted position from literal matching
            matchpos = pg_mbstrlen_with_len(queryText, curpos + 1) + newcursorpos;
        }
    }

    return matchpos;  // Return mapped position or 0 if not found
}
```