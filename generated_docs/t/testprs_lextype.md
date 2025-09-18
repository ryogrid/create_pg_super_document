# testprs_lextype

## Location
[src/test/modules/test_parser/test_parser.c:108-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_parser/test_parser.c#L108-L127)

## Overview
Provide lexical type information and descriptions for tokens recognized by the PostgreSQL test parser module.

## Definition
```c
Datum testprs_lextype(PG_FUNCTION_ARGS)
```

## Detailed Description
The testprs_lextype function returns a description of the lexical types (token types) that the test parser can recognize. It creates and populates an array of LexDescr structures that define the parser's vocabulary. The function specifically defines two token types: words (lexid 3) and blanks/spaces (lexid 12), with human-readable aliases and descriptions for each. The lexical IDs are intentionally compatible with PostgreSQL's default word parser to enable reuse of headline functionality. The array is terminated with a zero lexid to mark the end.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function interface (no input parameters needed)

## Dependencies
- Functions called/Symbols referenced:
  - LexDescr (structure type for lexical descriptions)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication function)
  - PG_RETURN_POINTER (PostgreSQL return macro)
- Called from (representative examples):
  - LexDescr (referenced in test parser lexical description)

## Notes and Other Information
- Returns array of LexDescr structures describing supported token types
- Token types: lexid 3 ("word") and lexid 12 ("blank"/spaces)
- Lexical IDs match default PostgreSQL word parser for headline function compatibility
- Array is null-terminated with lexid 0
- Provides both short aliases and longer descriptions for each token type
- Essential for PostgreSQL's text search and parsing infrastructure integration