# testprs_getlexeme

## Location
src/test/modules/test_parser/test_parser.c: 59 - 98

## Overview
Extract the next lexeme (token) from the input buffer during text parsing in PostgreSQL test parser module.

## Definition
```c
Datum testprs_getlexeme(PG_FUNCTION_ARGS)
```

## Detailed Description
The testprs_getlexeme function is the core parsing function that extracts individual lexemes (tokens) from the input text buffer. It implements a simple tokenization strategy that recognizes two types of tokens: spaces (blank type) and words. The function advances the parser position through the buffer and returns the token type, while also providing the token text and length through output parameters. When the end of input is reached, it returns type 0 to indicate completion.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function interface providing access to:
  - `PG_GETARG_POINTER(0)`: Pointer to ParserState structure
  - `PG_GETARG_POINTER(1)`: Output pointer to store token text pointer
  - `PG_GETARG_POINTER(2)`: Output pointer to store token length

## Dependencies
- Functions called/Symbols referenced:
  - [ParserState](../P/ParserState.md) (structure for maintaining parser state)
  - PG_GETARG_POINTER, PG_RETURN_INT32 (PostgreSQL function argument/return macros)
- Called from (representative examples):
  - LexDescr (referenced in test parser lexical description)

## Notes and Other Information
- Returns token types: 0 (end of input), 3 (word), 12 (blank/space)
- Implements simple space-delimited tokenization logic
- Updates parser position as it processes the input buffer
- Provides both token text pointer and length through output parameters
- Part of PostgreSQL's test infrastructure for parser functionality testing