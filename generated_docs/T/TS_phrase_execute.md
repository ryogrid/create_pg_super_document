# TS_phrase_execute

## Location
src/backend/utils/adt/tsvector_op.c: 1609 - 1853

## Overview
TS_phrase_execute is a recursive function that executes tsquery operations at or below an OP_PHRASE operator, handling text search execution at recursion levels where match locations are crucial for phrase matching and position-aware operations.

## Definition


## Detailed Description
This function is the core execution engine for PostgreSQL's text search phrase queries, designed to handle complex boolean logic while tracking lexeme positions for phrase matching. It recursively processes query trees containing OP_PHRASE, OP_AND, OP_OR, and OP_NOT operations, maintaining detailed position information required for proximity-based text searches.

The function implements sophisticated position semantics:
- For successful matches with npos > 0 and negate = false: query matches at specified positions only
- For npos > 0 and negate = true: query matches everywhere except specified positions  
- For npos = 0 and negate = true: query matches at all positions
- Returns a "width" value representing match width in lexemes minus one

Key behaviors include:
- Stack depth checking to prevent overflow during deep recursion
- Interrupt handling for query cancellation
- Position data management through ExecPhraseData structures
- Complex boolean logic handling with position propagation
- Phrase distance calculation and width computation

## Parameters / Member Variables
- : Pointer to the current QueryItem being processed in the query tree
- : Opaque argument passed to the TSExecuteCallback function
- : Execution flags controlling behavior (e.g., TS_EXEC_SKIP_NOT)
- : Callback function to check if a lexeme condition is satisfied
- : Pointer to ExecPhraseData structure for position information (NULL if positions not needed)

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - CHECK_FOR_INTERRUPTS
  - TS_phrase_output
  - chkcond (callback)
  - memset
  - elog
- Called from (representative examples):
  - TS_phrase_execute (recursive calls)
  - TS_execute_recurse
  - TS_execute_locations_recurse

## Notes and Other Information
- The function is designed to be recursively safe with stack depth monitoring
- Handles De Morgan's law transformations for negated boolean operations (\!L & \!R becomes \!(L | R))
- Position alignment logic ensures consistent width reporting across different operator types
- The width calculation follows the rule that positions represent match ends rather than starts when width > 0
- Critical for phrase search functionality where word proximity and order matter
- Returns TSTernaryValue (TS_YES, TS_NO, TS_MAYBE) to handle uncertain match scenarios