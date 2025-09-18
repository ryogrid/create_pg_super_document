# CleanQuerytext

## Location
src/backend/nodes/queryjumblefuncs.c: 66 - 104

## Overview
CleanQuerytext is a utility function that processes SQL query text by trimming it to the relevant portion and removing leading/trailing whitespace for query normalization purposes.

## Definition


## Detailed Description
CleanQuerytext takes a potentially multi-statement SQL source string and confines attention to the relevant part based on the provided location and length parameters. The function handles cases where the query location might be unknown (-1) and applies intelligent defaults. It also trims whitespace from both ends using PostgreSQL's scanner_isspace() function to match lexer behavior, ensuring consistency in query text processing for features like query ID generation.

## Parameters / Member Variables
- : Input SQL query string (potentially multi-statement)
- : Pointer to starting offset within the query string (input/output parameter)
- : Pointer to length of the relevant portion (input/output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - scanner_isspace (used for whitespace detection matching lexer behavior)
  - JumbleState (referenced in context)
- Called from (representative examples):
  - COMPUTE_QUERY_ID_REGRESS (macro in queryjumble.h)

## Notes and Other Information
- Returns a pointer to the cleaned query text within the original string
- Modifies location and len parameters to reflect the actual cleaned portion
- Uses scanner_isspace() instead of standard libc isspace() to maintain consistency with PostgreSQL's lexer
- Handles edge cases like unknown locations (-1) and zero/negative lengths gracefully
- Part of PostgreSQL's query jumbling infrastructure for query normalization