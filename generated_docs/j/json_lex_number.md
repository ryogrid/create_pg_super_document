# json_lex_number

## Location
src/common/jsonapi.c: 1946 - 2055

## Overview
A specialized lexical analyzer function that parses and validates JSON numeric tokens according to JSON specification rules for number format.

## Definition

```c
static inline JsonParseErrorType
json_lex_number(JsonLexContext *lex, const char *s,
				bool *num_err, size_t *total_len)
```
## Detailed Description
The  function implements precise parsing of JSON numeric literals according to the JSON specification. It validates and processes numbers consisting of up to four parts: an optional minus sign, integer digits, an optional decimal portion, and an optional exponent. The function strictly enforces JSON number formatting rules, such as prohibiting leading zeros in multi-digit integers and requiring digits after decimal points and exponents.

The parsing process follows JSON specification precisely:
1. **Leading sign**: Handled by caller (already processed)
2. **Integer part**: Single '0' or digits not starting with '0'
3. **Decimal part**: Optional period followed by one or more digits
4. **Exponent part**: Optional 'e'/'E', optional sign, one or more digits

The function supports incremental parsing for streaming scenarios and provides flexible error handling through optional output parameters. It also detects trailing alphanumeric garbage that should be considered part of the token for error reporting.

## Parameters / Member Variables
- : Pointer to JsonLexContext containing lexical state and input buffer information
- : Pointer to the beginning of the numeric token (after any leading minus sign)
- : Optional output parameter for error flag instead of raising errors directly  
- : Optional output parameter for total token length from input start

## Dependencies
- Functions called/Symbols referenced:
  - JSON_ALPHANUMERIC_CHAR (character classification macro for trailing garbage detection)
  - appendBinaryStringInfo (partial token buffer management for incremental parsing)
  - JSON error and success constants (JSON_INCOMPLETE, JSON_INVALID_TOKEN, JSON_SUCCESS)

- Called from (representative examples):
  - json_lex (main lexer for number tokens)
  - IsValidJsonNumber (number validation utility)

## Notes and Other Information
- Implements strict JSON number format validation per JSON specification
- Supports incremental parsing with partial token buffering for streaming inputs
- Provides flexible error handling modes: direct error return or error flag output
- Detects and includes trailing alphanumeric characters as part of invalid tokens
- Enforces JSON rules like no leading zeros, required digits after decimal/exponent markers
- Optimized as inline function for performance in the lexical analysis hot path
- Part of PostgreSQL's JSON infrastructure ensuring spec-compliant number parsing
- Handles both integer and floating-point JSON numbers including scientific notation
- Used for both parsing validation and token boundary determination