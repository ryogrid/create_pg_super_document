# TParserPosition

## Location
src/backend/tsearch/wparser_def.c: 229 - 239

## Overview
TParserPosition represents a snapshot of the parser's state and position within text being parsed by PostgreSQL's text search parser, supporting backtracking and state management during tokenization.

## Definition


## Detailed Description
TParserPosition is a crucial data structure in PostgreSQL's text search parser that maintains positional and state information during text tokenization. It forms a linked list structure that enables the parser to maintain a stack of positions, allowing for backtracking when the parser needs to reconsider parsing decisions. This is particularly important for complex text patterns where the parser might need to try different parsing paths.

The structure tracks both byte and character positions separately to handle multibyte character encodings correctly. It also maintains information about the current token being built and the parser's finite state machine state.

## Parameters / Member Variables
- : Current position in the input text measured in bytes
- : Current position in the input text measured in characters (important for multibyte encodings)
- : Length of the current character being processed (can be > 1 for multibyte characters)
- : Length of the token currently being constructed, measured in bytes
- : Length of the token currently being constructed, measured in characters
- : Current state of the parser's finite state machine (TParserState enum value)
- : Pointer to the previous position in the stack, enabling backtracking functionality
- : Pointer to the action item that caused this position to be pushed onto the stack

## Dependencies
- Functions called/Symbols referenced:
  - TParserState
  - TParserStateActionItem
- Called from (representative examples):
  - TParser (as state field)
  - newTParserPosition
  - TParserClose
  - TParserCopyClose
  - TParserGet

## Notes and Other Information
This structure is fundamental to the parser's ability to handle complex tokenization scenarios where lookahead and backtracking are necessary. The linked list design allows for efficient push/pop operations when the parser needs to save and restore positions. The separation of byte and character counting is essential for proper handling of Unicode and other multibyte character encodings in PostgreSQL's internationalization support.