# dsnowball_lexize

## Location
[src/backend/snowball/dict_snowball.c:270-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/dict_snowball.c#L270-L347)

## Overview
This function performs lexical analysis on input text using Snowball stemming algorithms, converting words to their stem forms while handling stopwords, encoding conversions, and length restrictions.

## Definition
```c
Datum dsnowball_lexize(PG_FUNCTION_ARGS)
```

## Detailed Description
The function is the core lexical analysis routine for Snowball dictionaries in PostgreSQL's text search system. It processes input text by first converting it to lowercase, then checking for various conditions: strings over 1000 bytes are returned as-is to prevent stemmer inefficiencies or crashes; empty strings or stopwords return NULL (indicating they should be ignored); otherwise, the text is passed through the Snowball stemming algorithm. The function handles encoding conversions when necessary, switching between server encoding and UTF-8 as required by the specific stemmer module.

## Parameters / Member Variables
- Function receives PG_FUNCTION_ARGS which contains:
  - `d`: Pointer to initialized DictSnowball structure containing stemmer configuration
  - `in`: Input text buffer to be processed
  - `len`: Length of the input text in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [lowerstr_with_len](../l/lowerstr_with_len.md)
  - [searchstoplist](../s/searchstoplist.md)
  - [pg_server_to_any](../p/pg_server_to_any.md)
  - [pg_any_to_server](../p/pg_any_to_server.md)
  - [SN_set_current](../S/SN_set_current.md)
  - [palloc0](../p/palloc0.md)
  - [pfree](../p/pfree.md)
  - [repalloc](../r/repalloc.md)
  - memcpy
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - PG_UTF8 (encoding constant)
- Called from (representative examples):
  - PostgreSQL text search framework (referenced by MININT)

## Notes and Other Information
- This is a PostgreSQL function following the standard PG function calling convention
- Implements a 1000-byte safety limit to protect against stemmer vulnerabilities (like Turkish stemmer recursion)
- Handles automatic encoding conversion between server encoding and UTF-8 when required by the stemmer
- Uses memory context switching to ensure stemmer operations occur in the dictionary's memory context
- Returns TSLexeme structure containing the processed lexeme or NULL for stopwords/empty strings
- The function is designed to never reject input as 'unknown' - all strings are processed in some form