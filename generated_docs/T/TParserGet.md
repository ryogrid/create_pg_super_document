# TParserGet

## Location
src/backend/tsearch/wparser_def.c: 1710 - 1877

## Overview
The core parsing engine function that processes text character by character using a state machine to identify and extract tokens from input text for PostgreSQL's full-text search functionality.

## Definition


## Detailed Description
TParserGet is the central function in PostgreSQL's word parser that implements a finite state automaton for tokenizing text. It processes the input string one character at a time, transitioning between parser states based on character classifications and predefined rules.

The function operates through these key stages:
1. **Character Processing**: Determines character length using  for multibyte character support
2. **State Machine Execution**: Uses a dispatch table () to find appropriate actions based on current state and character class
3. **Action Execution**: Performs state transitions, stack operations (push/pop), and token recognition
4. **Token Completion**: When A_BINGO flag is set, completes token extraction and returns true

The parser supports:
- Stack-based state management for complex parsing scenarios (nested structures)
- Multiple action flags for different operations (PUSH, POP, CLEAR, MERGE, etc.)
- Character class-based rule matching
- Special handler functions for complex parsing logic
- Comprehensive tracing support for debugging (when WPARSER_TRACE is enabled)

## Parameters / Member Variables
- : Pointer to TParser structure containing:
  - Current parsing state and position information
  - Input string data and character encoding details
  - Token extraction state and stack management

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (PostgreSQL interrupt handling)
  - pg_mblen (multibyte character length calculation)
  - newTParserPosition (stack state creation)
  - pfree (memory deallocation)
  - Various state constants (TPS_Base, TPS_Null, etc.)
  - Action flags (A_BINGO, A_POP, A_PUSH, A_CLEAR, A_MERGE, A_CLRALL, A_RERUN)
- Called from:
  - p_ishost (host validation parsing)
  - p_isURLPath (URL path validation parsing)
  - prsd_nexttoken (main token extraction interface)

## Notes and Other Information
- Returns  when a complete token is found (A_BINGO flag set),  when no more tokens available
- Implements sophisticated stack-based parsing for handling nested structures and backtracking
- Includes extensive debugging support via WPARSER_TRACE macro for development and troubleshooting
- Critical for PostgreSQL's text search performance as it processes all searchable text content
- Uses character class dispatch for efficient rule matching rather than character-by-character comparisons