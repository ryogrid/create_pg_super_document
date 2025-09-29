# json_lex

## Location
[src/common/jsonapi.c:1309-1671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L1309-L1671)

## Overview
The core lexical analyzer function that tokenizes JSON input, handling both streaming and incremental parsing while identifying and classifying JSON tokens.

## Definition

```c
JsonParseErrorType
json_lex(JsonLexContext *lex)
```
## Detailed Description
The  function is the central component of PostgreSQL's JSON lexical analysis system. It processes JSON input character by character to identify and classify tokens such as strings, numbers, literals (true/false/null), and structural punctuation (braces, brackets, commas, colons). The function supports both traditional parsing and incremental parsing for streaming JSON data.

Key functionality includes:
1. **Incremental parsing support**: Handles partial tokens across input chunks, maintaining state between calls
2. **Whitespace management**: Skips whitespace and tracks line numbers for error reporting
3. **Token classification**: Identifies all valid JSON token types through character analysis
4. **Partial token reconstruction**: Accumulates incomplete tokens across multiple input chunks
5. **Error detection**: Validates token structure and reports specific error locations

The function uses a state machine approach, examining the current character to determine token type and delegating to specialized lexers for complex tokens (strings, numbers). For incremental parsing, it maintains partial token buffers and handles token completion across input boundaries.

## Parameters / Member Variables
- : Pointer to JsonLexContext containing lexical state including input position, token boundaries, line numbers, and incremental parsing state

## Dependencies
- Functions called/Symbols referenced:
  - [resetStringInfo](../r/resetStringInfo.md) (partial token buffer management)
  - appendStringInfoCharMacro (partial token building)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md) (bulk partial token addition)
  - [json_lex_string](json_lex_string.md) (string token parsing)
  - [json_lex_number](json_lex_number.md) (number token parsing)
  - JSON_ALPHANUMERIC_CHAR (character classification macro)
  - JSON token type constants
  - memcmp (literal token comparison)

- Called from (representative examples):
  - [pg_parse_json](../p/pg_parse_json.md) (main parsing entry point)
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md) (incremental parsing)
  - [parse_scalar](../p/parse_scalar.md), parse_object_field, parse_object, parse_array (recursive parser components)
  - [lex_expect](../l/lex_expect.md) (token validation)

## Notes and Other Information
- Supports both complete and incremental JSON parsing modes
- Maintains line number tracking for detailed error reporting
- Handles UTF-8 encoding considerations through input_encoding context
- Implements partial token buffering for streaming scenarios where tokens span input chunks
- Uses recursive self-calls to process completed partial tokens
- Integrates with specialized token parsers for complex token types
- Central to PostgreSQL's JSON processing infrastructure across multiple modules
- Error handling preserves exact token positions for meaningful error messages
- Supports all JSON specification token types including structural punctuation and literals

## Simplified Source

```c
JsonParseErrorType json_lex(JsonLexContext *lex) {
    const char *s;
    const char *const end = lex->input + lex->input_length;
    JsonParseErrorType result;

    // Handle completed partial tokens from incremental parsing
    if (lex->incremental && lex->inc_state->partial_completed) {
        resetStringInfo(&(lex->inc_state->partial_token));
        lex->token_terminator = lex->input;
        lex->inc_state->partial_completed = false;
    }

    s = lex->token_terminator;

    // Complex partial token handling for incremental parsing
    if (lex->incremental && lex->inc_state->partial_token.len) {
        // ... [Partial token reconstruction logic] ...
        // This involves accumulating characters across input chunks
        // until we have a complete token, then recursively calling json_lex
    }

    // Skip whitespace and track line numbers
    while (s < end && (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r')) {
        if (*s++ == '\n') {
            ++lex->line_number;
            lex->line_start = s;
        }
    }
    lex->token_start = s;

    // Determine token type by examining first character
    if (s >= end) {
        lex->token_type = JSON_TOKEN_END;
    } else {
        switch (*s) {
            case '{': lex->token_type = JSON_TOKEN_OBJECT_START; break;
            case '}': lex->token_type = JSON_TOKEN_OBJECT_END; break;
            case '[': lex->token_type = JSON_TOKEN_ARRAY_START; break;
            case ']': lex->token_type = JSON_TOKEN_ARRAY_END; break;
            case ',': lex->token_type = JSON_TOKEN_COMMA; break;
            case ':': lex->token_type = JSON_TOKEN_COLON; break;
            case '"':
                // Parse string token
                result = json_lex_string(lex);
                if (result != JSON_SUCCESS) return result;
                lex->token_type = JSON_TOKEN_STRING;
                break;
            case '-':
            case '0'...'9':
                // Parse number token
                result = json_lex_number(lex, s, NULL, NULL);
                if (result != JSON_SUCCESS) return result;
                lex->token_type = JSON_TOKEN_NUMBER;
                break;
            default:
                // Handle literals: true, false, null
                const char *p;
                for (p = s; p < end && JSON_ALPHANUMERIC_CHAR(*p); p++);

                if (p - s == 4 && memcmp(s, "true", 4) == 0)
                    lex->token_type = JSON_TOKEN_TRUE;
                else if (p - s == 5 && memcmp(s, "false", 5) == 0)
                    lex->token_type = JSON_TOKEN_FALSE;
                else if (p - s == 4 && memcmp(s, "null", 4) == 0)
                    lex->token_type = JSON_TOKEN_NULL;
                else
                    return JSON_INVALID_TOKEN;

                lex->token_terminator = p;
        }

        // Update token boundaries for single-character tokens
        if (*s != '"' && (*s < '0' || *s > '9') && *s != '-' &&
            !JSON_ALPHANUMERIC_CHAR(*s)) {
            lex->prev_token_terminator = lex->token_terminator;
            lex->token_terminator = s + 1;
        }
    }

    return JSON_SUCCESS;
}
```