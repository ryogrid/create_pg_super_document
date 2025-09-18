# JsonLexContext

## Location
[src/include/common/jsonapi.h:89-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/jsonapi.h#L89-L107)

## Overview
JsonLexContext is a structure that maintains the state and context information for JSON lexical analysis, serving as the primary data structure for parsing JSON text and tracking position, tokens, and parsing state.

## Definition
```c
typedef struct JsonLexContext
{
    const char *input;
    size_t      input_length;
    int         input_encoding;
    const char *token_start;
    const char *token_terminator;
    const char *prev_token_terminator;
    bool        incremental;
    JsonTokenType token_type;
    int         lex_level;
    bits32      flags;
    int         line_number;        /* line number, starting from 1 */
    const char *line_start;        /* where that line starts within input */
    JsonParserStack *pstack;
    JsonIncrementalState *inc_state;
    StringInfo  strval;
    StringInfo  errormsg;
} JsonLexContext;
```

## Detailed Description
JsonLexContext is the core lexical context structure used throughout PostgreSQL's JSON parsing infrastructure. It encapsulates all necessary state information for parsing JSON text, including input positioning, token tracking, error handling, and support for both complete and incremental parsing modes. The structure maintains current parsing position, line tracking for error reporting, lexical nesting level, and various flags that control parsing behavior.

## Parameters / Member Variables
- `input`: Pointer to the JSON text being parsed
- `input_length`: Total length of the input JSON text
- `input_encoding`: Character encoding of the input text
- `token_start`: Pointer to the beginning of the current token
- `token_terminator`: Pointer to the end of the current token
- `prev_token_terminator`: Pointer to the end of the previous token (for backtracking)
- `incremental`: Boolean flag indicating whether parsing is incremental
- `token_type`: Type of the current token being processed
- `lex_level`: Current nesting level in the JSON structure (objects/arrays)
- `flags`: Bitfield containing various parsing control flags
- `line_number`: Current line number in the input (1-based, for error reporting)
- `line_start`: Pointer to the start of the current line
- `pstack`: Stack for tracking nested JSON structures during parsing
- `inc_state`: State information specific to incremental parsing
- `strval`: StringInfo buffer for accumulating string values
- `errormsg`: StringInfo buffer for error message construction

## Dependencies
- Functions called/Symbols referenced:
  - [JsonTokenType](JsonTokenType.md)
  - bits32
  - JsonParserStack
  - JsonIncrementalState
- Called from (representative examples):
  - [makeJsonLexContextCstringLen](../m/makeJsonLexContextCstringLen.md)
  - makeJsonLexContextIncremental
  - [pg_parse_json](../p/pg_parse_json.md)
  - json_lex
  - [parse_scalar](../p/parse_scalar.md)
  - [json_in](../j/json_in.md)
  - [jsonb_from_cstring](../j/jsonb_from_cstring.md)

## Notes and Other Information
This structure is central to PostgreSQL's JSON processing capabilities and is used across multiple modules including json.c, jsonb.c, jsonfuncs.c, and the common jsonapi.c. It supports both complete and incremental parsing modes, with the incremental flag and inc_state member enabling streaming JSON processing. The structure includes comprehensive error tracking with line numbers and position information for detailed error reporting. The pstack member enables proper handling of nested JSON structures by maintaining the parsing context stack.