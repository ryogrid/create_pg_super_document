# prsd_lextype

## Location
[src/backend/tsearch/wparser_def.c:1878-1895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L1878-L1895)

## Overview
A PostgreSQL function that returns metadata about all lexical token types supported by the default text search parser, providing both numeric IDs and human-readable descriptions for each token type.

## Definition

```c
Datum
prsd_lextype(PG_FUNCTION_ARGS)
```
## Detailed Description
prsd_lextype is a PostgreSQL built-in function that serves as the lextype interface for the default word parser. It creates and returns an array of LexDescr structures containing comprehensive information about all supported token types.

The function performs these operations:
1. Allocates memory for an array of LexDescr structures (LASTNUM + 1 entries)
2. Iterates through all valid token types (1 to LASTNUM, which is 23)
3. For each token type, populates:
   - : The numeric token type identifier
   - : A short string alias from the tok_alias array
   - : A human-readable description from the lex_descr array
4. Adds a terminating entry with lexid = 0
5. Returns the array as a PostgreSQL Datum

This function is typically called by PostgreSQL's text search system when users query for parser information using functions like  or when examining parser capabilities.

## Parameters / Member Variables
- Returns: Pointer to array of LexDescr structures

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](palloc.md) (PostgreSQL memory allocation)
  - [pstrdup](pstrdup.md) (PostgreSQL string duplication)
  - tok_alias (array of token type aliases)
  - lex_descr (array of token type descriptions)
  - LASTNUM (constant defining maximum token type number, value 23)
  - LexDescr (structure type for token metadata)
  - PG_RETURN_POINTER (PostgreSQL return macro)
- Called from:
  - PostgreSQL function call interface (no direct code references found)

## Notes and Other Information
- This is a PostgreSQL interface function exposed to SQL users for introspection of parser capabilities
- The LASTNUM constant (23) indicates the parser supports 23 different token types
- Memory allocated with palloc will be automatically freed by PostgreSQL's memory context system
- The returned array is null-terminated (lexid = 0 in final entry)
- Used internally by PostgreSQL's text search framework for parser metadata queries