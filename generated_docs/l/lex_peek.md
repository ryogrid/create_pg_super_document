# lex_peek

## Location
[src/common/jsonapi.c:238-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L238-L249)

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
  - [JsonLexContext](../J/JsonLexContext.md) (structure access)
  - [JsonTokenType](../J/JsonTokenType.md) (return type)
- Called from (representative examples):
  - [lex_expect](lex_expect.md)
  - [pg_parse_json](../p/pg_parse_json.md)
  - [json_count_array_elements](../j/json_count_array_elements.md)
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md)
  - [parse_scalar](../p/parse_scalar.md)
  - [parse_object_field](../p/parse_object_field.md)
  - [parse_object](../p/parse_object.md)
  - [parse_array_element](../p/parse_array_element.md)
  - [parse_array](../p/parse_array.md)

## Notes and Other Information
This function is crucial for implementing predictive parsing in the JSON parser. It allows the parser to examine upcoming tokens to make parsing decisions without consuming the token, which is essential for error recovery and proper JSON grammar implementation. The inline nature ensures minimal performance overhead for this frequently called operation.

## Simplified Source

```c
static inline JsonTokenType lex_peek(JsonLexContext *lex) {
    // Return the current token type without advancing the lexer
    return lex->token_type;
}
```