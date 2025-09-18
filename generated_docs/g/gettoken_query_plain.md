# gettoken_query_plain

## Location
src/backend/utils/adt/tsquery.c: 510 - 530

## Overview
A static tokenizer function that extracts tokens from plain text input for tsquery parsing, treating the entire input buffer as a single value token.

## Definition


## Detailed Description
This function serves as a simplified tokenizer for plain text tsquery input. Unlike complex query parsers that handle operators, parentheses, and special syntax, this function treats the entire input buffer as a single token of type PT_VAL. It's designed for scenarios where the input should be interpreted as a plain text search term without any query operators or structure.

The function reads the entire remaining content of the input buffer, sets the appropriate output parameters, and advances the parser state. It always returns either PT_VAL (if there's content) or PT_END (if the buffer is empty), making it suitable for simple text-based queries.

## Parameters / Member Variables
- : TSQueryParserState containing the current parsing context and input buffer
- : Output parameter for operator type (set but not used in plain text parsing)
- : Output parameter receiving the length of the extracted token
- : Output parameter receiving a pointer to the token string
- : Output parameter for token weight (always set to 0 for plain text)
- : Output parameter for prefix matching flag (always set to false)

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
  - PT_END (token type constant)
  - PT_VAL (token type constant)
  - TSQueryParserState (parser state structure)
  - int8 (PostgreSQL type alias)
- Called from (representative examples):
  - parse_tsquery

## Notes and Other Information
- This function is part of PostgreSQL's text search functionality for processing tsquery input
- It's specifically designed for plain text queries where no complex parsing logic is needed
- The function consumes the entire remaining buffer in one operation, making it unsuitable for structured query syntax
- Weight and prefix parameters are always set to default values (0 and false respectively) since plain text queries don't support these features
- The function advances the parser state by incrementing the count and moving the buffer pointer to the end