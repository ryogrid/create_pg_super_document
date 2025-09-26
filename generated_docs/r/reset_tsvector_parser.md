# reset_tsvector_parser

## Location
[src/backend/utils/adt/tsvector_parser.c:81-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_parser.c#L81-L89)

## Overview
Reinitializes an existing parser state to parse a new input string while preserving the original configuration and error reporting context.

## Definition

```c
void
reset_tsvector_parser(TSVectorParseState state, char *input)
```
## Detailed Description
This function allows reusing an existing TSVectorParseState object to parse a different input string without recreating the entire parser state. It only updates the parsing buffer pointer while keeping all other configuration settings (flags, error context, buffer allocations) intact. The bufstart field, which is used for error reporting, remains unchanged to preserve the original error context.

## Parameters / Member Variables
- : The existing TSVectorParseState object to reset
- : The new input string to parse

## Dependencies
- Functions called/Symbols referenced:
  - [TSVectorParseState](../T/TSVectorParseState.md)
- Called from (representative examples):
  - [gettoken_query_standard](../g/gettoken_query_standard.md) (src/backend/utils/adt/tsquery.c:324)
  - [gettoken_query_websearch](../g/gettoken_query_websearch.md) (src/backend/utils/adt/tsquery.c:453)

## Notes and Other Information
This function is designed for efficiency when parsing multiple related strings with the same parsing configuration. It avoids the overhead of destroying and recreating parser state objects. The original bufstart is preserved for consistent error reporting that references the initial input context.