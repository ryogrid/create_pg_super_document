# parse_or_operator

## Location
src/backend/utils/adt/tsquery.c: 244 - 285

## Overview
A static function that determines whether an "OR" literal in a websearch-style tsquery should be treated as a logical OR operator.

## Definition


## Detailed Description
The parse_or_operator function is specifically designed for websearch_to_tsquery() functionality to distinguish between the literal word "OR" and the logical OR operator. It performs several validation checks to ensure that "OR" appears as a standalone operator rather than as part of a larger word. The function checks that "OR" is followed by whitespace and eventually by another operand, and that it's not part of a compound word (containing hyphens, underscores, or alphanumeric characters immediately after "OR").

The function implements a lookahead mechanism to verify that there is indeed an operand following the OR keyword, even if separated by whitespace. If all validations pass, it advances the parser buffer by 2 characters (past "OR") and returns true.

## Parameters / Member Variables
- : Parser state containing the current buffer position and parsing context

## Dependencies
- Functions called/Symbols referenced:
  - TSQueryParserState
  - pg_strncasecmp
  - t_iseq
  - t_isalnum
  - pg_mblen
  - t_isspace
  - ts_tokentype
- Called from (representative examples):
  - gettoken_query_websearch

## Notes and Other Information
- Case-insensitive comparison for "OR" literal using pg_strncasecmp
- Handles multi-byte character sequences properly with pg_mblen
- Part of PostgreSQL's websearch-style query parsing, which provides a more user-friendly query syntax
- Returns false if OR appears at the end of input or is part of a compound word
- Used to differentiate between searching for the word "or" versus using OR as a logical operator