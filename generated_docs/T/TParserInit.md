# TParserInit

## Location
src/backend/tsearch/wparser_def.c: 289 - 345

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
  - palloc0 (zero-initialized memory allocation)
  - pg_database_encoding_max_length (get max character length for database encoding)
  - pg_mb2wchar_with_len (convert multibyte to wide chars for C locale)
  - char2wchar (convert multibyte to wide chars for other locales)
  - newTParserPosition (create initial parser position)
  - TPS_Base (initial parser state constant)

- Called from (representative examples):
  - prsd_start (text search parser start function)

## Notes and Other Information
- This is a static function, only accessible within the wparser_def.c module
- Automatically handles single-byte vs multi-byte character encoding differences
- For multi-byte encodings, creates wide character representations of the input string
- Uses different wide character conversion functions depending on whether database locale is C
- Initial parser state is always set to TPS_Base
- Memory allocation uses palloc0 ensuring zero-initialized structure
- Includes conditional compilation for WPARSER_TRACE debugging support