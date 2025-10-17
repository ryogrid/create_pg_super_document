# gettoken_query_websearch

## Location
[src/backend/utils/adt/tsquery.c:398-509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L398-L509)

## Overview
A static function that tokenizes and parses websearch-style tsquery expressions, providing a more user-friendly syntax compared to standard PostgreSQL tsquery format.

## Definition

```c
static ts_tokentype
gettoken_query_websearch(TSQueryParserState state, int8 *operator,
						 int *lenval, char **strval,
						 int16 *weight, bool *prefix)
```
## Detailed Description
The gettoken_query_websearch function implements parsing logic for PostgreSQL's websearch_to_tsquery() functionality, which provides a simpler, more intuitive query syntax similar to web search engines. Unlike the standard tsquery parser, this function supports quoted phrases, uses hyphen (-) for negation instead of exclamation mark (!), automatically inserts AND operators between adjacent terms, and uses the parse_or_operator function to intelligently distinguish between "OR" as a literal word versus a logical operator.

Key features include:
- Quoted phrases are treated as single tokens
- Implicit AND operators between consecutive operands
- Minus sign (-) for NOT operations
- Case-insensitive "OR" operator recognition
- Automatic handling of operator characters that would be invalid in standard mode

## Parameters / Member Variables
- `state`: Parser state containing current position, parsing state, and context information
- `*operator`: Output parameter receiving the operator type (OP_AND, OP_OR, OP_NOT)
- `*lenval`: Output parameter for the length of parsed string values
- `**strval`: Output parameter for parsed string values (operands)
- `*weight`: Output parameter for weight information (not used in websearch mode)
- `*prefix`: Output parameter for prefix matching (not used in websearch mode)
## Dependencies
- Functions called/Symbols referenced:
  - [TSQueryParserState](../T/TSQueryParserState.md)
  - t_iseq
  - [t_isspace](../t/t_isspace.md)
  - ISOPERATOR
  - [reset_tsvector_parser](../r/reset_tsvector_parser.md)
  - [gettoken_tsvector](gettoken_tsvector.md)
  - SOFT_ERROR_OCCURRED
  - [pushStop](../p/pushStop.md)
  - [parse_or_operator](../p/parse_or_operator.md)
  - [pg_mblen](../p/pg_mblen.md)
- Called from (representative examples):
  - [parse_tsquery](../p/parse_tsquery.md)

## Notes and Other Information
- Designed for websearch_to_tsquery() which provides Google-like search syntax
- No support for weight modifiers or prefix operators (*, A, B, C, D)
- Automatically inserts AND operators between adjacent terms without explicit operators
- Uses pushStop() to handle cases where parsing ends without a proper operand
- Ignores invalid operator characters instead of raising errors (more forgiving than standard mode)
- Quoted strings bypass normal tsvector parsing and are treated as literal phrases
- The ISOPERATOR macro helps identify characters that should be skipped in websearch mode

## Simplified Source

```c
static ts_tokentype gettoken_query_websearch(TSQueryParserState state, int8 *operator,
                                           int *lenval, char **strval,
                                           int16 *weight, bool *prefix) {
    *weight = 0;
    *prefix = false;

    while (true) {
        switch (state->state) {
            case WAITFIRSTOPERAND:
            case WAITOPERAND:
                // Handle minus sign for NOT operator
                if (t_iseq(state->buf, '-')) {
                    state->buf++;
                    state->state = WAITOPERAND;
                    *operator = OP_NOT;
                    return PT_OPR;
                }
                // Handle quoted phrases
                else if (t_iseq(state->buf, '"')) {
                    state->buf++;  // Skip opening quote
                    *strval = state->buf;

                    // Find closing quote or end of string
                    while (*state->buf != '\0' && !t_iseq(state->buf, '"'))
                        state->buf++;
                    *lenval = state->buf - *strval;

                    if (*state->buf != '\0')  // Skip closing quote
                        state->buf++;

                    state->state = WAITOPERATOR;
                    state->count++;
                    return PT_VAL;
                }
                // Skip invalid operators
                else if (ISOPERATOR(state->buf)) {
                    state->buf++;
                    state->state = WAITOPERAND;
                    continue;
                }
                // Parse regular operands
                else if (!t_isspace(state->buf)) {
                    reset_tsvector_parser(state->valstate, state->buf);
                    if (gettoken_tsvector(state->valstate, strval, lenval, NULL, NULL, &state->buf)) {
                        state->state = WAITOPERATOR;
                        return PT_VAL;
                    }
                    // Handle end of input or errors
                    if (state->state == WAITFIRSTOPERAND)
                        return PT_END;
                    else {
                        pushStop(state);
                        return PT_END;
                    }
                }
                break;

            case WAITOPERATOR:
                if (*state->buf == '\0') {
                    return PT_END;
                }
                // Check for OR operator
                else if (parse_or_operator(state)) {
                    state->state = WAITOPERAND;
                    *operator = OP_OR;
                    return PT_OPR;
                }
                // Skip other operators
                else if (ISOPERATOR(state->buf)) {
                    state->buf++;
                    continue;
                }
                // Insert implicit AND between operands
                else if (!t_isspace(state->buf)) {
                    state->state = WAITOPERAND;
                    *operator = OP_AND;
                    return PT_OPR;
                }
                break;
        }

        // Advance to next character
        state->buf += pg_mblen(state->buf);
    }
}
```