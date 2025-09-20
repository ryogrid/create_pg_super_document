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
- : Parser state containing current position, parsing state, and context information
- : Output parameter receiving the operator type (OP_AND, OP_OR, OP_NOT)
- : Output parameter for the length of parsed string values
- : Output parameter for parsed string values (operands)
- : Output parameter for weight information (not used in websearch mode)
- : Output parameter for prefix matching (not used in websearch mode)

## Dependencies
- Functions called/Symbols referenced:
  - TSQueryParserState
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