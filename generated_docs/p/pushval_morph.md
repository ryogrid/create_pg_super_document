# pushval_morph

## Location
src/backend/tsearch/to_tsany.c: 492 - 578

## Overview
A static callback function used for morphological parsing that processes text values, lexizes them through dictionaries, and pushes appropriate query elements to the TSQuery parser stack.

## Definition
```c
static void pushval_morph(Datum opaque, TSQueryParserState state, char *strval, int lenval, int16 weight, bool prefix)
```

## Detailed Description
This function implements the core logic for morphological analysis during TSQuery parsing. It takes input text, processes it through the configured text search dictionaries to produce lexemes and their variants, then constructs appropriate query tree nodes. The function handles complex scenarios including stopword placeholders, multiple word variants, and maintains proper operator precedence. Words belonging to the same morphological variant are connected with AND operators, while different variants are connected with OR operators. The function also manages position-based operators for phrase searches and inserts placeholders for removed stopwords.

## Parameters / Member Variables
- `opaque` (Datum): Pointer to MorphOpaque structure containing configuration data (cfg_id and qoperator)
- `state` (TSQueryParserState): Parser state for building the query tree
- `strval` (char *): Input text string to be morphologically analyzed
- `lenval` (int): Length of the input string
- `weight` (int16): Weight to assign to the resulting lexemes
- `prefix` (bool): Whether to treat input as a prefix search

## Dependencies
- Functions called/Symbols referenced:
  - TSQueryParserState
  - ParsedText
  - [MorphOpaque](../M/MorphOpaque.md)
  - ParsedWord
  - [parsetext](parsetext.md)
  - [pushStop](pushStop.md)
  - [pushOperator](pushOperator.md)
  - [pushValue](pushValue.md)
  - TSL_PREFIX
  - OP_AND
  - OP_OR
- Called from (representative examples):
  - [to_tsquery_byid](../t/to_tsquery_byid.md)
  - [plainto_tsquery_byid](plainto_tsquery_byid.md)
  - [phraseto_tsquery_byid](phraseto_tsquery_byid.md)
  - [websearch_to_tsquery_byid](../w/websearch_to_tsquery_byid.md)

## Notes and Other Information
- Central function for morphological processing in PostgreSQL's text search query parsing
- Handles complex operator precedence: individual lexemes within variants are ANDed, variants are ORed, and positions are connected by the configured qoperator
- Inserts stopword placeholders to maintain proper phrase search behavior when stopwords are removed
- Memory management includes proper cleanup of allocated ParsedWord structures
- Supports both regular and prefix search modes depending on TSL_PREFIX flag and prefix parameter
- The function builds a query tree that accurately represents the morphological relationships in the original text