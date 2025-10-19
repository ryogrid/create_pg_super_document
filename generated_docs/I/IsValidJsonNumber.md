# IsValidJsonNumber

## Location
[src/common/jsonapi.c:272-325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L272-L325)

## Overview
A utility function that validates whether a given string represents a valid JSON number according to the JSON specification.

## Definition
bool IsValidJsonNumber(const char *str, size_t len)

## Detailed Description
The IsValidJsonNumber function provides a standalone way to validate JSON number syntax without performing a full JSON parse. It creates a temporary JsonLexContext and uses the internal json_lex_number function to validate the number format. The function handles both positive and negative numbers by checking for a leading minus sign and adjusting the input accordingly before validation. It ensures that the entire input string is consumed during number parsing, meaning no extraneous characters are present.

## Parameters / Member Variables
- str: Pointer to the character string to validate (need not be null-terminated)
- len: Length of the string to validate

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md) (temporary structure creation)
  - [json_lex_number](../j/json_lex_number.md) (internal number parsing function)
- Called from (representative examples):
  - No references found in the current codebase

## Notes and Other Information
This function is particularly useful for validating JSON numbers in contexts where a full JSON parse is not needed. The function handles the JSON number specification correctly, including support for negative numbers by preprocessing the minus sign. The implementation creates a dummy lexer context specifically for validation purposes, making it safe to use independently of other JSON parsing operations. Note that the function requires casting away const-ness of the input string, which is documented as an ugly but necessary implementation detail.

## Simplified Source

```c
bool IsValidJsonNumber(const char *str, size_t len)
{
    bool numeric_error;
    size_t total_len;
    JsonLexContext dummy_lex;

    // Basic validation
    if (len <= 0)
        return false;

    // Initialize dummy lexer context
    dummy_lex.incremental = false;
    dummy_lex.inc_state = NULL;
    dummy_lex.pstack = NULL;

    // Handle negative numbers by skipping leading '-'
    if (*str == '-') {
        dummy_lex.input = str + 1;
        dummy_lex.input_length = len - 1;
    } else {
        dummy_lex.input = str;
        dummy_lex.input_length = len;
    }

    dummy_lex.token_start = dummy_lex.input;

    // Validate the number using JSON lexer
    json_lex_number(&dummy_lex, dummy_lex.input, &numeric_error, &total_len);

    // Return true if no error and entire string was consumed
    return (!numeric_error) && (total_len == dummy_lex.input_length);
}
```