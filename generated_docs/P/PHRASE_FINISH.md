# PHRASE_FINISH

## Location
src/backend/utils/adt/tsquery.c: 172 - 243

## Overview
An enumeration constant representing the final state in the phrase operator parsing state machine within PostgreSQL's tsquery parser.

## Definition


## Detailed Description
PHRASE_FINISH is one of four states in the finite state machine used by the parse_phrase_operator function to parse phrase operators in tsquery expressions. This state represents the successful completion of parsing a phrase operator syntax like '<N>' where N is a distance value. When the parser reaches this state, it means the phrase operator has been fully validated and parsed, and the distance value can be extracted and returned.

The state machine progression is:
1. PHRASE_OPEN: Expecting '<'
2. PHRASE_DIST: Parsing distance number or '-'
3. PHRASE_CLOSE: Expecting '>'
4. PHRASE_FINISH: Parsing completed successfully

## Parameters / Member Variables
- Part of a local enumeration within the parse_phrase_operator function
- Used as a state indicator in the parsing state machine

## Dependencies
- Functions called/Symbols referenced:
  - Used within parse_phrase_operator function context
- Called from (representative examples):
  - Referenced in state transitions within parse_phrase_operator

## Notes and Other Information
- This is a local enumeration constant, not a global symbol
- Represents the successful completion state of phrase operator parsing
- When this state is reached, the parser extracts the distance value and updates the parser state buffer position
- Part of PostgreSQL's full-text search query parsing infrastructure for handling proximity operators