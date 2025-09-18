# to_tsquery_byid

## Location
src/backend/tsearch/to_tsany.c: 579 - 604

## Overview
A PostgreSQL function that converts text input to a TSQuery using a specified text search configuration, with morphological parsing and phrase-based word position matching.

## Definition
```c
Datum to_tsquery_byid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function creates a TSQuery from input text using a specified text search configuration ID. It configures morphological parsing with the OP_PHRASE operator, which ensures that complex morphological forms require exact word position matching in the target TSVector. When complex morphs are connected, all their constituent words are arranged in a phrase sequence. The function delegates the actual parsing work to `parse_tsquery` with the `pushval_morph` callback for handling morphological analysis.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]` (Oid): Text search configuration ID to use for parsing
- `PG_FUNCTION_ARGS[1]` (text *): Input text to be converted to TSQuery

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID
  - PG_GETARG_TEXT_PP
  - TSQuery
  - [MorphOpaque](../M/MorphOpaque.md)
  - OP_PHRASE
  - [parse_tsquery](../p/parse_tsquery.md)
  - text_to_cstring
  - [pushval_morph](../p/pushval_morph.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - PG_RETURN_TSQUERY
- Called from (representative examples):
  - [to_tsquery](to_tsquery.md)

## Notes and Other Information
- Uses OP_PHRASE as the default operator for connecting morphological variants, ensuring precise positional matching
- Part of PostgreSQL's text search functionality for creating structured queries from natural language input
- The MorphOpaque structure carries both the configuration ID and the chosen query operator (OP_PHRASE)
- Implements the PostgreSQL function calling conventions for SQL-callable functions
- The choice of OP_PHRASE operator makes this function suitable for applications requiring exact phrase matching rather than loose word proximity