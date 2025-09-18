# lex_expect

## Location
src/common/jsonapi.c: 250 - 258

## Overview
A static inline function that conditionally advances the JSON lexer to the next token if the current token matches the expected token type, otherwise reports a parse error.

## Definition
static inline JsonParseErrorType lex_expect(JsonParseContext ctx, JsonLexContext *lex, JsonTokenType token)

## Detailed Description
The lex_expect function implements conditional token consumption in the JSON parser. It first uses lex_peek to check if the current lookahead token matches the expected token type. If there is a match, it advances the lexer by calling json_lex and returns the result. If the current token does not match the expected token, it reports a parse error using report_parse_error. This function is fundamental to implementing the predictive parsing strategy used throughout the JSON parser.

## Parameters / Member Variables
- ctx: JsonParseContext for error reporting context
- lex: Pointer to the JsonLexContext structure containing the current parser state
- token: The expected JsonTokenType that should match the current lookahead token

## Dependencies
- Functions called/Symbols referenced:
  - JsonParseContext (parameter type)
  - JsonLexContext (parameter type)  
  - JsonTokenType (parameter type)
  - lex_peek (to check current token)
  - json_lex (to advance lexer)
  - report_parse_error (for error handling)
- Called from (representative examples):
  - pg_parse_json
  - json_count_array_elements
  - parse_object_field
  - parse_object
  - parse_array

## Notes and Other Information
This function encapsulates the common pattern of checking for an expected token and either consuming it or reporting an error. It is a key building block for implementing the JSON grammar rules and ensures consistent error handling throughout the parser. The function returns JsonParseErrorType to indicate success or the specific type of parsing error encountered.