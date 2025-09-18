# PsqlScanStateData

## Location
src/include/fe_utils/psqlscan_int.h: 84 - 132

## Overview
PsqlScanStateData is the central state structure that contains all working state of PostgreSQL's lexical scanner, enabling re-entrant and multi-instance lexer operations for psql and related utilities.

## Definition


## Detailed Description
PsqlScanStateData is the comprehensive state container for PostgreSQL's lexical scanning system, designed to support re-entrant lexer operations essential for handling nested include files and multiple simultaneous scanning contexts. This structure encapsulates all the information needed to maintain scanning state between lexer calls, enabling sophisticated features like variable substitution, multibyte encoding support, and complex SQL parsing rules.

The structure is organized into several logical groups: flex integration state, buffer management for variable expansion, encoding handling for international character sets, persistent parsing state for complex SQL constructs, and callback mechanisms for extensibility. This design allows psql to handle complex scenarios like nested variable expansions, dollar-quoted strings, and proper parsing of stored procedure definitions with BEGIN/END blocks.

## Parameters / Member Variables
- : yyscan_t holding flex's internal state for this particular scanner instance
- : PQExpBuffer for accumulating the current output being constructed
- : StackElem pointer to the top of the variable expansion buffer stack
- : YY_BUFFER_STATE for the outer-level input buffer (not variable expansion buffers)
- : Pointer to the start of the outer-level input buffer string
- : Current input line being processed at the outer level
- : Integer identifier for the character encoding currently in use
- : Boolean indicating whether the current encoding is "safe" (all bytes >= 0x80)
- : Boolean indicating whether string literals follow standard SQL rules
- : Actual string data that flex is scanning from the current buffer
- : Original unmodified data for the current buffer (before FF substitutions)
- : Integer representing yylex's starting and finishing state for persistence
- : State condition before encountering an end quote
- : Current nesting depth within parentheses
- : Current nesting depth within /* */ style comments
- : String holding the current dollar-quote delimiter (e.g., "foo" for $foo$)
- : Number of identifiers seen since the start of the current statement
- : Array storing the first few identifiers to detect BEGIN/END patterns
- : Current nesting depth of BEGIN/END block pairs
- : Pointer to callback functions for variable resolution and other extensions
- : Void pointer passed through to callback functions for context

## Dependencies
- Functions called/Symbols referenced:
  - yyscan_t (flex scanner type)
  - PQExpBuffer (PostgreSQL string buffer type)
  - [StackElem](../S/StackElem.md) (variable expansion stack element)
  - [YY_BUFFER_STATE](../Y/YY_BUFFER_STATE.md) (flex buffer state)
  - [PsqlScanCallbacks](PsqlScanCallbacks.md) (callback function structure)
- Called from (representative examples):
  - [PsqlScanState](PsqlScanState.md) (typedef alias for this structure)
  - psqlscan_create (creates instances)
  - psqlscan_destroy (destroys instances)

## Notes and Other Information
- Essential for supporting re-entrant lexer operations and multiple simultaneous scanning contexts
- Handles complex multibyte encoding scenarios by substituting 0xFF for unsafe bytes during scanning
- Supports sophisticated SQL parsing including dollar-quoted strings and nested comments
- Manages variable expansion through a stack-based buffer system
- Designed to work with multiple compatible lexers (psqlscan.l, psqlscanslash.l) that can share state
- The structure must persist between lexer calls to maintain parsing context across input lines
- Critical for proper handling of PostgreSQL-specific SQL extensions and psql meta-commands
- Used exclusively in frontend utilities, not in the backend database server