# lex_expect

## Location
[src/common/jsonapi.c:250-258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L250-L258)

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
  - [JsonLexContext](../J/JsonLexContext.md) (parameter type)  
  - [JsonTokenType](../J/JsonTokenType.md) (parameter type)
  - [lex_peek](lex_peek.md) (to check current token)
  - [json_lex](../j/json_lex.md) (to advance lexer)
  - [report_parse_error](../r/report_parse_error.md) (for error handling)
- Called from (representative examples):
  - [pg_parse_json](../p/pg_parse_json.md)
  - [json_count_array_elements](../j/json_count_array_elements.md)
  - [parse_object_field](../p/parse_object_field.md)
  - [parse_object](../p/parse_object.md)
  - [parse_array](../p/parse_array.md)

## Notes and Other Information
This function encapsulates the common pattern of checking for an expected token and either consuming it or reporting an error. It is a key building block for implementing the JSON grammar rules and ensures consistent error handling throughout the parser. The function returns JsonParseErrorType to indicate success or the specific type of parsing error encountered.