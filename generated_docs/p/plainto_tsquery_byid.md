# plainto_tsquery_byid

## Location
src/backend/tsearch/to_tsany.c: 617 - 641

## Overview
Converts plain text to a TSQuery using a specific text search configuration by OID, treating the entire input as a phrase where all words must match using AND operator.

## Definition
```c
Datum plainto_tsquery_byid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the internal implementation for converting plain text to a PostgreSQL text search query (TSQuery) using a specified text search configuration identified by its OID. Unlike regular tsquery parsing, this function treats the entire input text as a single morphological unit and connects all resulting terms with the AND operator, meaning all words in the input must be present in matching documents (regardless of their positions).

The function uses the P_TSQ_PLAIN flag during parsing, which instructs the parser to treat the whole input as plain text rather than interpreting special query syntax. This makes it suitable for user input that should be searched literally without worrying about query syntax characters.

## Parameters / Member Variables
- : Text search configuration OID that defines the language-specific rules for tokenization and stemming
- : Input text to be converted into a TSQuery

## Dependencies
- Functions called/Symbols referenced:
  - TSQuery (return type)
  - MorphOpaque (data structure for morphological operations)
  - OP_AND (operator constant)
  - parse_tsquery (core parsing function)
  - text_to_cstring (text conversion utility)
  - pushval_morph (morphological processing callback)
  - P_TSQ_PLAIN (parsing flag constant)
- Called from (representative examples):
  - plainto_tsquery

## Notes and Other Information
- This is a PostgreSQL internal function (Datum-returning) designed to be called through the SQL function interface
- The OP_AND operator ensures that all terms in the query must match, making it more restrictive than phrase queries
- The function is typically wrapped by the user-facing plainto_tsquery() function which uses the default text search configuration
- Part of PostgreSQL's full-text search functionality introduced to provide user-friendly query creation from plain text input