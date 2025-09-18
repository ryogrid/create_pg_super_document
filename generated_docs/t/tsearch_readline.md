# tsearch_readline

## Location
[src/backend/tsearch/ts_locale.c:157-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_locale.c#L157-L201)

## Overview
Reads the next line from a text search data file, handling UTF-8 validation and character encoding conversion to the database encoding.

## Definition


## Detailed Description
This function reads one line from a file that was previously opened with tsearch_readline_begin(). It expects the input file to be in UTF-8 encoding and automatically converts the content to the database's current encoding if necessary. The function handles memory management carefully, ensuring that returned strings are properly allocated and that previous line data is cleaned up.

The function increments the line number counter for error reporting purposes and maintains a current line buffer that can be referenced in error messages. It returns a newly allocated string that the caller is responsible for freeing.

## Parameters / Member Variables
- : Pointer to the tsearch_readline_state structure initialized by tsearch_readline_begin()

## Dependencies
- Functions called/Symbols referenced:
  - pg_get_line_buf
  - [pg_any_to_server](../p/pg_any_to_server.md)
  - [pstrdup](../p/pstrdup.md)
  - [pfree](../p/pfree.md)
  - PG_UTF8 (constant)
- Called from (representative examples):
  - [dsynonym_init](../d/dsynonym_init.md)
  - [thesaurusRead](thesaurusRead.md)
  - [NIImportDictionary](../N/NIImportDictionary.md)
  - NIImportOOAffixes
  - NIImportAffixes
  - [readstoplist](../r/readstoplist.md)

## Notes and Other Information
- Returns NULL when end of file is reached
- Always returns a freshly allocated string via pstrdup() for memory safety
- Validates input as UTF-8 and converts to database encoding if needed
- Increments line number counter for accurate error reporting
- Manages memory carefully to avoid over-allocation in long-lived dictionary structures
- The returned string must be freed by the caller using pfree()
- Maintains curline pointer for error context reporting
- Used in conjunction with tsearch_readline_begin() and tsearch_readline_end()