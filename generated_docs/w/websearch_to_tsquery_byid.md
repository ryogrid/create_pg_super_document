# websearch_to_tsquery_byid

## Location
src/backend/tsearch/to_tsany.c: 692 - 717

## Overview
Converts web search-style query text to a TSQuery using a specific text search configuration by OID, supporting web search syntax like quoted phrases and boolean operators.

## Definition
```c
Datum websearch_to_tsquery_byid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function converts web search-style query text into a PostgreSQL text search query (TSQuery) using a specified text search configuration identified by its OID. Unlike the plain text functions, this function uses the P_TSQ_WEB parsing flag, which enables web search syntax interpretation including quoted phrases, boolean operators, and more sophisticated query construction.

The function uses OP_PHRASE as the default operator, which means that when complex morphs are given in quotes, all their words are connected into a phrase sequence that requires exact positional matching. This allows for more nuanced query construction that resembles modern web search interfaces.

The P_TSQ_WEB flag enables parsing of web search syntax such as quoted phrases for exact matching, plus and minus operators for inclusion/exclusion, and other common web search conventions.

## Parameters / Member Variables
- `PG_GETARG_OID(0)`: Text search configuration OID that defines the language-specific rules for tokenization and stemming
- `PG_GETARG_TEXT_PP(1)`: Input text in web search syntax to be converted into a TSQuery

## Dependencies
- Functions called/Symbols referenced:
  - MorphOpaque (data structure for morphological operations)
  - TSQuery (return type)
  - OP_PHRASE (phrase operator constant for quoted terms)
  - parse_tsquery (core parsing function)
  - text_to_cstring (text conversion utility)
  - pushval_morph (morphological processing callback)
  - P_TSQ_WEB (web search parsing flag constant)
  - PG_RETURN_TSQUERY (PostgreSQL return macro)
- Called from (representative examples):
  - websearch_to_tsquery

## Notes and Other Information
- This is a PostgreSQL internal function designed to be called through the SQL function interface
- The P_TSQ_WEB flag enables advanced web search syntax parsing, making it more flexible than plain text functions
- Supports quoted phrases for exact matching while allowing other web search conventions
- The OP_PHRASE operator ensures that quoted terms maintain their positional relationships
- Part of PostgreSQL's full-text search functionality designed to provide familiar web search interface semantics
- The function is typically wrapped by the user-facing websearch_to_tsquery() function which uses the default text search configuration
- More sophisticated than both plainto_tsquery_byid and phraseto_tsquery_byid as it can handle mixed query syntax