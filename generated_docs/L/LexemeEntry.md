# LexemeEntry

## Location
[src/backend/tsearch/wparser.c:151-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser.c#L151-L157)

## Overview
LexemeEntry is a structure used in PostgreSQL's text search system to represent individual lexemes (tokens) extracted by text parsers, storing both the token type and the actual lexeme text.

## Definition


## Detailed Description
LexemeEntry serves as a container for parsed tokens in PostgreSQL's text search functionality. It is used internally by the word parser interface to store the results of text tokenization. Each LexemeEntry represents a single token discovered during text parsing, combining the token's classification (type) with its actual textual content (lexeme). This structure is primarily used as part of dynamic arrays in PrsStorage structures to accumulate all tokens found during parsing operations.

The structure is designed for temporary storage during parsing operations and is typically allocated in arrays that can be dynamically resized as more tokens are discovered. The lexeme field points to a null-terminated string copy of the token text, which is separately allocated and managed.

## Parameters / Member Variables
- : An integer representing the token type classification as determined by the text parser
- : A pointer to a null-terminated string containing the actual token text

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple data structure)
- Called from (representative examples):
  - [prs_setup_firstcall](../p/prs_setup_firstcall.md) (used in PrsStorage.list allocation)
  - [prs_process_call](../p/prs_process_call.md) (accessed to retrieve token data)

## Notes and Other Information
- Used exclusively within src/backend/tsearch/wparser.c as part of the text search parser interface
- Memory for the lexeme field is allocated separately using palloc() and must be freed with pfree()
- Typically used in conjunction with PrsStorage structure which maintains dynamic arrays of LexemeEntry
- Part of PostgreSQL's full-text search infrastructure, specifically the standard interface to word parsers
- The structure is designed for internal use and is not exposed directly to SQL-level functions