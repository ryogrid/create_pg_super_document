# lex_peek

## Location
src/common/jsonapi.c: 238 - 249

## Overview
A static inline function that returns the current lookahead token type from a JSON lexer context without advancing the lexer position.

## Definition
static inline JsonTokenType lex_peek(JsonLexContext *lex)

## Detailed Description
The lex_peek function provides a simple way to examine the current token in the JSON lexer without consuming it. This is essential for parsing logic that needs to make decisions based on the current token type before deciding whether to advance the parser. The function simply returns the token_type field from the JsonLexContext structure, making it a zero-cost operation for lookahead parsing.

## Parameters / Member Variables
- lex: Pointer to the JsonLexContext structure containing the current parser state

## Dependencies
- Functions called/Symbols referenced:
  - JsonLexContext (structure access)
  - JsonTokenType (return type)
- Called from (representative examples):
  - lex_expect
  - pg_parse_json
  - json_count_array_elements
  - pg_parse_json_incremental
  - parse_scalar
  - parse_object_field
  - parse_object
  - parse_array_element
  - parse_array

## Notes and Other Information
This function is crucial for implementing predictive parsing in the JSON parser. It allows the parser to examine upcoming tokens to make parsing decisions without consuming the token, which is essential for error recovery and proper JSON grammar implementation. The inline nature ensures minimal performance overhead for this frequently called operation.