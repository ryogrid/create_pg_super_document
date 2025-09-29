# pg_parse_json_incremental

## Location
[src/common/jsonapi.c:650-1007](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L650-L1007)

## Overview
A non-recursive top-down parser for incremental JSON processing that uses the Dragon Book Algorithm 4.3, allowing JSON to be parsed in chunks rather than requiring the complete input at once.

## Definition

```c
struct_action ostart = sem->object_start;
```
## Detailed Description
pg_parse_json_incremental implements a table-driven, non-recursive parser specifically designed for incremental JSON processing. Unlike the recursive descent parser used in pg_parse_json, this parser uses a prediction stack to manage parsing state, allowing it to handle JSON input that arrives in chunks. The parser uses semantic action markers placed strategically in the prediction stack to trigger appropriate callbacks at the correct parsing moments. It supports the complete JSON grammar including nested objects and arrays while maintaining parsing context across multiple function calls.

## Parameters / Member Variables
- : JsonLexContext pointer configured for incremental parsing via makeJsonLexContextIncremental()
- : JsonSemAction pointer containing semantic action function pointers that match those used in recursive descent parsing
- : Pointer to the JSON chunk to be processed (does not need to be null-terminated)
- : Length of the current JSON chunk in bytes
- : Boolean flag indicating whether this is the final chunk of JSON data

## Dependencies
- Functions called/Symbols referenced:
  - [json_lex](../j/json_lex.md) (for token lexing)
  - [lex_peek](../l/lex_peek.md) (for token lookahead)
  - [have_prediction](../h/have_prediction.md)/pop_prediction/push_prediction (prediction stack management)
  - [inc_lex_level](../i/inc_lex_level.md)/dec_lex_level (nesting level tracking)
  - [set_fname](../s/set_fname.md)/get_fname/set_fnull/get_fnull (field name and null value tracking)
  - [report_parse_error](../r/report_parse_error.md) (error reporting)
- Called from (representative examples):
  - [pg_parse_json](pg_parse_json.md) (src/common/jsonapi.c:542)
  - [json_parse_manifest_incremental_chunk](../j/json_parse_manifest_incremental_chunk.md) (src/common/parse_manifest.c:193)
  - [main](../m/main.md) functions in test modules for incremental parsing

## Notes and Other Information
This parser is slower than the recursive descent approach but necessary for incremental parsing scenarios. It requires the lexing context to be set up with incremental mode enabled, returning JSON_INVALID_LEXER_TYPE otherwise. The parser maintains a prediction stack with semantic action markers to ensure callbacks occur at the appropriate parsing moments. It enforces nesting depth limits (JSON_TD_MAX_STACK) and handles partial token processing across chunk boundaries. The semantic actions function identically to those in the recursive parser, maintaining compatibility between parsing approaches.

## Simplified Source

```c
JsonParseErrorType
pg_parse_json_incremental(JsonLexContext *lex, JsonSemAction *sem,
                         const char *json, size_t len, bool is_last)
{
    JsonTokenType tok;
    JsonParseErrorType result;
    JsonParseContext ctx = JSON_PARSE_VALUE;
    JsonParserStack *pstack = lex->pstack;

    if (!lex->incremental)
        return JSON_INVALID_LEXER_TYPE;

    // Set up input for this chunk
    lex->input = lex->token_terminator = lex->line_start = json;
    lex->input_length = len;
    lex->inc_state->is_last_chunk = is_last;

    // Get initial token
    result = json_lex(lex);
    if (result != JSON_SUCCESS)
        return result;
    tok = lex_peek(lex);

    // Initialize prediction stack if empty
    if (!have_prediction(pstack))
    {
        td_entry goal = TD_ENTRY(JSON_PROD_GOAL);
        push_prediction(pstack, goal);
    }

    // Main parsing loop using table-driven approach
    while (have_prediction(pstack))
    {
        char top = pop_prediction(pstack);
        td_entry entry;

        if (top == tok)
        {
            // Terminal matches - advance to next token
            if (tok < JSON_TOKEN_END)
            {
                result = json_lex(lex);
                if (result != JSON_SUCCESS)
                    return result;
                tok = lex_peek(lex);
            }
        }
        else if (IS_NT(top) && (entry = td_parser_table[OFS(top)][tok]).prod != NULL)
        {
            // Non-terminal production found - push RHS onto stack
            push_prediction(pstack, entry);
        }
        else if (IS_SEM(top))
        {
            // Execute semantic action
            switch (top)
            {
                case JSON_SEM_OSTART:
                    if (lex->lex_level >= JSON_TD_MAX_STACK)
                        return JSON_NESTING_TOO_DEEP;
                    if (sem->object_start != NULL)
                    {
                        result = (*sem->object_start)(sem->semstate);
                        if (result != JSON_SUCCESS)
                            return result;
                    }
                    inc_lex_level(lex);
                    break;

                case JSON_SEM_OEND:
                    dec_lex_level(lex);
                    if (sem->object_end != NULL)
                    {
                        result = (*sem->object_end)(sem->semstate);
                        if (result != JSON_SUCCESS)
                            return result;
                    }
                    break;

                case JSON_SEM_ASTART:
                    if (lex->lex_level >= JSON_TD_MAX_STACK)
                        return JSON_NESTING_TOO_DEEP;
                    if (sem->array_start != NULL)
                    {
                        result = (*sem->array_start)(sem->semstate);
                        if (result != JSON_SUCCESS)
                            return result;
                    }
                    inc_lex_level(lex);
                    break;

                case JSON_SEM_AEND:
                    dec_lex_level(lex);
                    if (sem->array_end != NULL)
                    {
                        result = (*sem->array_end)(sem->semstate);
                        if (result != JSON_SUCCESS)
                            return result;
                    }
                    break;

                // Additional semantic actions for field handling,
                // scalar processing, etc. (simplified for brevity)

                default:
                    break;
            }
        }
        else
        {
            // Parse error - determine context and report
            return report_parse_error(ctx, lex);
        }
    }

    return JSON_SUCCESS;
}
```