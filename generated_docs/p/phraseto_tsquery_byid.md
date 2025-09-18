# phraseto_tsquery_byid

## Location
src/backend/tsearch/to_tsany.c: 655 - 679

## Overview
Converts plain text to a TSQuery using a specific text search configuration by OID, treating the input as a phrase where word positions must match exactly.

## Definition
```c
Datum phraseto_tsquery_byid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function converts plain text input into a PostgreSQL text search query (TSQuery) using a specified text search configuration identified by its OID. Unlike plainto_tsquery_byid which uses the AND operator, this function uses the OP_PHRASE operator, which requires that words appear in the exact same positions as they do in the input text.

The function treats the entire input text as a single morphological unit using the P_TSQ_PLAIN flag, but connects the resulting terms with the PHRASE operator. This means that for a document to match, it must contain all the words from the input in the exact same sequence and positions, making it more restrictive than plain text queries but useful for exact phrase matching.

## Parameters / Member Variables
- `PG_GETARG_OID(0)`: Text search configuration OID that defines the language-specific rules for tokenization and stemming
- `PG_GETARG_TEXT_PP(1)`: Input text to be converted into a phrase TSQuery

## Dependencies
- Functions called/Symbols referenced:
  - TSQuery (return type)
  - [MorphOpaque](../M/MorphOpaque.md) (data structure for morphological operations)
  - OP_PHRASE (phrase operator constant)
  - [parse_tsquery](parse_tsquery.md) (core parsing function)
  - text_to_cstring (text conversion utility)
  - [pushval_morph](pushval_morph.md) (morphological processing callback)
  - P_TSQ_PLAIN (parsing flag constant)
  - PG_RETURN_TSQUERY (PostgreSQL return macro)
- Called from (representative examples):
  - [phraseto_tsquery](phraseto_tsquery.md)

## Notes and Other Information
- This is a PostgreSQL internal function designed to be called through the SQL function interface
- The OP_PHRASE operator enforces positional matching, making it ideal for exact phrase searches
- More restrictive than plainto_tsquery_byid as it requires exact word positioning rather than just presence
- The function is typically wrapped by the user-facing phraseto_tsquery() function which uses the default text search configuration
- Part of PostgreSQL's full-text search functionality for precise phrase matching in documents