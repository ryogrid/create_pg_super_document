# TS_execute

## Location
src/backend/utils/adt/tsvector_op.c: 1854 - 1870

## Overview
TS_execute is a high-level wrapper function that evaluates tsquery boolean expressions, providing a simplified interface for text search execution without requiring position information.

## Definition


## Detailed Description
This function serves as the primary entry point for executing tsquery expressions in PostgreSQL's text search system. It provides a boolean result interface, abstracting away the complexity of the underlying ternary logic system used internally. The function delegates the actual execution to TS_execute_recurse and converts its TSTernaryValue result to a simple boolean.

Key behavior:
- Returns true for both TS_YES and TS_MAYBE results from the recursive execution
- TS_MAYBE results can only occur when TS_EXEC_PHRASE_NO_POS flag is used
- Provides a clean boolean interface for callers who don't need position information
- Handles all types of tsquery operations through delegation

The function is designed for use cases where only the match/no-match result is needed, without caring about specific lexeme positions or phrase matching details.

## Parameters / Member Variables
- : Pointer to the first QueryItem in the tsquery expression tree
- : Opaque argument passed through to the TSExecuteCallback function
- : Execution control flags (bitmask from ts_utils.h)
- : Callback function that checks whether a primitive lexeme value is present

## Dependencies
- Functions called/Symbols referenced:
  - TS_execute_recurse
  - TS_NO (comparison)
- Called from (representative examples):
  - hlCover (in wparser_def.c)
  - gtsvector_consistent (in tsgistidx.c)
  - Cover (in tsrank.c)
  - ts_match_vq

## Notes and Other Information
- This is the standard interface for most text search operations that don't require position data
- The conversion from TSTernaryValue to bool means that uncertain matches (TS_MAYBE) are treated as positive matches
- More efficient than position-aware variants when only boolean results are needed
- Part of PostgreSQL's full-text search infrastructure, commonly used in GiST index operations and ranking functions
- The simplicity of this interface makes it suitable for most user-facing text search operations