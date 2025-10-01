# pg_parse_json

## Location
[src/common/jsonapi.c:522-587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L522-L587)

## Overview
The primary publicly visible entry point for PostgreSQL's JSON parser that processes JSON input using a lexing context and semantic action callbacks.

## Definition

```c
JsonParseErrorType
pg_parse_json(JsonLexContext *lex, JsonSemAction *sem)
```
## Detailed Description
pg_parse_json serves as the main interface for parsing JSON data in PostgreSQL. It accepts a pre-configured lexing context and semantic action structure, then performs recursive descent parsing to process JSON objects, arrays, or scalar values. The function supports two parsing modes: a standard recursive parser and an optional non-recursive parser (when FORCE_JSON_PSTACK is defined) for validation purposes. The parser handles the complete JSON grammar including nested structures and validates proper JSON syntax while invoking appropriate semantic actions during parsing.

## Parameters / Member Variables
- : JsonLexContext pointer containing the lexical analysis context, input data, and parsing state
- : JsonSemAction pointer containing function pointers to semantic action routines and state object for callback execution

## Dependencies
- Functions called/Symbols referenced:
  - [pg_parse_json_incremental](pg_parse_json_incremental.md) (for non-recursive mode)
  - [json_lex](../j/json_lex.md) (for initial token lexing)
  - [lex_peek](../l/lex_peek.md) (for token lookahead)
  - [parse_object](parse_object.md) (for JSON object parsing)
  - [parse_array](parse_array.md) (for JSON array parsing) 
  - [parse_scalar](parse_scalar.md) (for JSON scalar value parsing)
  - [lex_expect](../l/lex_expect.md) (for end-of-input validation)
- Called from (representative examples):
  - [json_validate](../j/json_validate.md) (src/backend/utils/adt/json.c:1687)
  - [pg_parse_json_or_errsave](pg_parse_json_or_errsave.md) (src/backend/utils/adt/jsonfuncs.c:522)
  - [json_parse_manifest](../j/json_parse_manifest.md) (src/common/parse_manifest.c:256)

## Notes and Other Information
The function includes conditional compilation support for FORCE_JSON_PSTACK which enables the non-recursive parser for testing and validation. This mode may produce different error messages related to stack depth but should otherwise behave identically. The parser requires the lexing context to be properly initialized via makeJsonLexContext() before use and will return JSON_INVALID_LEXER_TYPE if called with an incremental lexer in standard mode.

## Simplified Source

```c
JsonParseErrorType
pg_parse_json(JsonLexContext *lex, JsonSemAction *sem)
{
#ifdef FORCE_JSON_PSTACK
    // Initialize non-recursive parser mode
    lex->incremental = true;
    lex->inc_state = palloc0(sizeof(JsonIncrementalState));
    initStringInfo(&(lex->inc_state->partial_token));

    // Set up parser stack
    lex->pstack = palloc(sizeof(JsonParserStack));
    lex->pstack->stack_size = JS_STACK_CHUNK_SIZE;
    lex->pstack->prediction = palloc(JS_STACK_CHUNK_SIZE * JS_MAX_PROD_LEN);
    lex->pstack->pred_index = 0;
    lex->pstack->fnames = palloc(JS_STACK_CHUNK_SIZE * sizeof(char *));
    lex->pstack->fnull = palloc(JS_STACK_CHUNK_SIZE * sizeof(bool));

    return pg_parse_json_incremental(lex, sem, lex->input, lex->input_length, true);
#else
    JsonTokenType tok;
    JsonParseErrorType result;

    // Check lexer type compatibility
    if (lex->incremental)
        return JSON_INVALID_LEXER_TYPE;

    // Get initial token
    result = json_lex(lex);
    if (result != JSON_SUCCESS)
        return result;

    tok = lex_peek(lex);

    // Parse by recursive descent based on first token
    switch (tok)
    {
        case JSON_TOKEN_OBJECT_START:
            result = parse_object(lex, sem);
            break;
        case JSON_TOKEN_ARRAY_START:
            result = parse_array(lex, sem);
            break;
        default:
            result = parse_scalar(lex, sem);  // JSON can be bare scalar
    }

    // Verify end of input
    if (result == JSON_SUCCESS)
        result = lex_expect(JSON_PARSE_END, lex, JSON_TOKEN_END);

    return result;
#endif
}
```