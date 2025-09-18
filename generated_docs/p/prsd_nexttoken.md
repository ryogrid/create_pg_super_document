# prsd_nexttoken

## Location
src/backend/tsearch/wparser_def.c: 1902 - 1917

## Overview
A PostgreSQL function that extracts the next token from an initialized text parser, returning the token type and providing the token text and length through output parameters.

## Definition


## Detailed Description
prsd_nexttoken is a PostgreSQL built-in function that serves as the primary token extraction interface for the default word parser. It works in conjunction with an initialized TParser instance (created by prsd_start) to iteratively extract tokens from the input text.

The function performs these operations:
1. Extracts the TParser instance from the first function argument
2. Gets pointers to output parameters for token text and token length  
3. Calls the internal TParserGet function to advance the parser and extract the next token
4. If a token is found:
   - Sets the token text pointer to point to the token in the input string
   - Sets the token length to the number of bytes in the token
   - Returns the token type ID as an integer
5. If no more tokens are available, returns 0

The function is designed to be called repeatedly until it returns 0, allowing complete tokenization of the input text. Each call advances the parser state and extracts the next sequential token according to PostgreSQL's default parsing rules.

## Parameters / Member Variables
- Argument 0: Pointer to initialized TParser structure  
- Argument 1: Pointer to char* variable that will receive the token text pointer
- Argument 2: Pointer to int variable that will receive the token length
- Returns: Integer token type ID (> 0) or 0 if no more tokens

## Dependencies
- Functions called/Symbols referenced:
  - [TParserGet](../T/TParserGet.md) (core parsing engine function)
  - [TParser](../T/TParser.md) (parser structure type)
  - PG_GETARG_POINTER (PostgreSQL argument extraction macro)
  - PG_RETURN_INT32 (PostgreSQL return value macro)
- Called from:
  - PostgreSQL function call interface (no direct code references found)

## Notes and Other Information  
- This is a PostgreSQL interface function exposed to the text search framework for token extraction
- The function provides a streaming interface - tokens are extracted one at a time rather than all at once
- The returned token text pointer points directly into the original input string (no copying)
- Token types correspond to the lexical categories defined in the parser (see prsd_lextype for details)
- Used as part of the standard text search parser workflow: prsd_start → prsd_nexttoken (repeatedly) → cleanup
- Return value of 0 indicates end of tokenization; positive values indicate valid token types