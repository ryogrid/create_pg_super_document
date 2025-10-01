# parse_object

## Location
[src/common/jsonapi.c:1114-1187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L1114-L1187)

## Overview
A recursive descent parsing function that processes JSON object constructs, handling the parsing of curly-brace-enclosed sequences of key-value pairs separated by commas.

## Definition

```c
struct_action ostart = sem->object_start;
```
## Detailed Description
The  function implements JSON object parsing within PostgreSQL's JSON API infrastructure. It processes JSON objects as sequences of object fields (key-value pairs) surrounded by curly braces and separated by commas. The function manages nesting levels, invokes semantic actions for object start/end events, and coordinates with the lexer to consume tokens appropriately.

The parsing follows a standard recursive descent approach:
1. Calls the semantic action for object start if provided
2. Increments the lexical nesting level
3. Expects and consumes the opening brace token
4. Parses object fields in a loop, handling comma separators
5. Expects and consumes the closing brace token  
6. Decrements the lexical nesting level
7. Calls the semantic action for object end if provided

The function includes stack depth checking in non-frontend builds to prevent stack overflow during deeply nested JSON parsing.

## Parameters / Member Variables
- : Pointer to JsonLexContext containing lexical analysis state including current token position and nesting level
- : Pointer to JsonSemAction structure containing semantic action callbacks for object start/end events

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - [lex_peek](../l/lex_peek.md) (token lookahead)
  - [json_lex](../j/json_lex.md) (token consumption)
  - [parse_object_field](parse_object_field.md) (recursive field parsing)
  - [lex_expect](../l/lex_expect.md) (expected token validation)
  - [report_parse_error](../r/report_parse_error.md) (error reporting)
  - JSON token types and parse error constants

- Called from (representative examples):
  - [pg_parse_json](pg_parse_json.md) (main JSON parsing entry point)
  - [parse_object_field](parse_object_field.md) (recursive object nesting)
  - [parse_array_element](parse_array_element.md) (objects within arrays)

## Notes and Other Information
- Manages lexical nesting level () to track JSON structure depth
- Supports empty objects (immediate closing brace after opening brace)
- Integrates with semantic action framework allowing customizable object processing
- Stack depth checking prevents stack overflow in deeply nested JSON structures
- Error handling preserves parse context for meaningful error reporting
- Part of PostgreSQL's common JSON parsing infrastructure used across multiple modules

## Simplified Source

```c
static JsonParseErrorType
parse_object(JsonLexContext *lex, JsonSemAction *sem)
{
    json_struct_action ostart = sem->object_start;
    json_struct_action oend = sem->object_end;
    JsonTokenType tok;
    JsonParseErrorType result;

    // Call object start semantic action
    if (ostart != NULL) {
        result = (*ostart)(sem->semstate);
        if (result != JSON_SUCCESS)
            return result;
    }

    // Increment nesting level
    lex->lex_level++;

    // Consume opening brace
    result = json_lex(lex);
    if (result != JSON_SUCCESS)
        return result;

    // Parse object fields
    tok = lex_peek(lex);
    switch (tok) {
        case JSON_TOKEN_STRING:
            result = parse_object_field(lex, sem);
            // Parse additional fields separated by commas
            while (result == JSON_SUCCESS && lex_peek(lex) == JSON_TOKEN_COMMA) {
                result = json_lex(lex);  // consume comma
                if (result != JSON_SUCCESS)
                    break;
                result = parse_object_field(lex, sem);
            }
            break;
        case JSON_TOKEN_OBJECT_END:
            break;  // Empty object
        default:
            result = report_parse_error(JSON_PARSE_OBJECT_START, lex);
    }
    if (result != JSON_SUCCESS)
        return result;

    // Expect closing brace
    result = lex_expect(JSON_PARSE_OBJECT_NEXT, lex, JSON_TOKEN_OBJECT_END);
    if (result != JSON_SUCCESS)
        return result;

    // Decrement nesting level
    lex->lex_level--;

    // Call object end semantic action
    if (oend != NULL) {
        result = (*oend)(sem->semstate);
        if (result != JSON_SUCCESS)
            return result;
    }

    return JSON_SUCCESS;
}
```