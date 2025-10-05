# TParserInit

## Location
[src/backend/tsearch/wparser_def.c:289-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L289-L345)

## Overview
Initializes a new TParser structure for parsing text in PostgreSQL's text search word parser, handling both single-byte and multi-byte character encodings.

## Definition
```c
static TParser *TParserInit(char *str, int len)
```

## Detailed Description
TParserInit creates and initializes a new TParser structure used for parsing text in PostgreSQL's text search functionality. The function handles the complexity of multi-byte character encodings by determining whether to use wide character processing based on the database encoding's maximum character length.

The function allocates memory for the parser structure and sets up the input string information. If the database encoding requires multi-byte characters (charmaxlen > 1), it converts the input string to wide characters using either pg_mb2wchar_with_len for C locale or char2wchar for other locales. The parser state is initialized to TPS_Base using a new TParserPosition structure.

## Parameters / Member Variables
- `str`: Input string to be parsed (multibyte string)
- `len`: Length of the input string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md) (get max character length for database encoding)
  - [pg_mb2wchar_with_len](../p/pg_mb2wchar_with_len.md) (convert multibyte to wide chars for C locale)
  - [char2wchar](../c/char2wchar.md) (convert multibyte to wide chars for other locales)
  - [newTParserPosition](../n/newTParserPosition.md) (create initial parser position)
  - TPS_Base (initial parser state constant)

- Called from (representative examples):
  - [prsd_start](../p/prsd_start.md) (text search parser start function)

## Notes and Other Information
- This is a static function, only accessible within the wparser_def.c module
- Automatically handles single-byte vs multi-byte character encoding differences
- For multi-byte encodings, creates wide character representations of the input string
- Uses different wide character conversion functions depending on whether database locale is C
- Initial parser state is always set to TPS_Base
- Memory allocation uses palloc0 ensuring zero-initialized structure
- Includes conditional compilation for WPARSER_TRACE debugging support

## Simplified Source

```c
static TParser *
TParserInit(char *str, int len)
{
    // Allocate and initialize parser structure
    TParser *prs = (TParser *) palloc0(sizeof(TParser));

    // Set up basic input string information
    prs->charmaxlen = pg_database_encoding_max_length();
    prs->str = str;
    prs->lenstr = len;

    // Handle multi-byte character encodings
    if (prs->charmaxlen > 1) {
        prs->usewide = true;
        if (database_ctype_is_c) {
            // Convert to wide chars for C locale
            prs->pgwstr = (pg_wchar *) palloc(sizeof(pg_wchar) * (prs->lenstr + 1));
            pg_mb2wchar_with_len(prs->str, prs->pgwstr, prs->lenstr);
        } else {
            // Convert to wide chars for other locales
            prs->wstr = (wchar_t *) palloc(sizeof(wchar_t) * (prs->lenstr + 1));
            char2wchar(prs->wstr, prs->lenstr + 1, prs->str, prs->lenstr, 0);
        }
    } else {
        prs->usewide = false;
    }

    // Initialize parser state to base state
    prs->state = newTParserPosition(NULL);
    prs->state->state = TPS_Base;

    return prs;
}
```