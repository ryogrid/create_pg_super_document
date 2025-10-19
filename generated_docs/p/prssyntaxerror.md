# prssyntaxerror

## Location
[src/backend/utils/adt/tsvector_parser.c:142-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_parser.c#L142-L175)

## Overview
Handles syntax errors during tsvector or tsquery parsing by reporting appropriate error messages through the error context system.

## Definition

```c
static bool
prssyntaxerror(TSVectorParseState state)
```
## Detailed Description
This static function is responsible for generating and reporting syntax errors that occur during parsing operations. It uses the error context system to handle both hard and soft error scenarios. The function generates different error messages depending on whether the parser is in tsquery mode or tsvector mode, providing context-appropriate error reporting. In soft error situations, it returns false as a convenience for the caller.

## Parameters / Member Variables
- `state`: The TSVectorParseState containing the parser context and error information
## Dependencies
- Functions called/Symbols referenced:
  - errsave
  - [TSVectorParseState](../T/TSVectorParseState.md)
  - ERRCODE_SYNTAX_ERROR
- Called from (representative examples):
  - PRSSYNTAXERROR macro (src/backend/utils/adt/tsvector_parser.c:139)

## Notes and Other Information
This function is typically invoked through the PRSSYNTAXERROR macro rather than called directly. It supports both hard error handling (throws exceptions) and soft error handling (returns false) depending on the error context configuration. The error messages include the full input string for better debugging context.

## Simplified Source

```c
static bool
prssyntaxerror(TSVectorParseState state)
{
    // Report syntax error with appropriate message based on parser type
    if (state->is_tsquery) {
        errsave(state->escontext,
               (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("syntax error in tsquery: \"%s\"", state->bufstart)));
    } else {
        errsave(state->escontext,
               (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("syntax error in tsvector: \"%s\"", state->bufstart)));
    }

    // Return false for soft error handling
    return false;
}
```