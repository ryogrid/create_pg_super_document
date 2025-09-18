# checkmatchall

## Location
src/backend/regex/regc_nfa.c: 3097 - 3276

## Overview
The checkmatchall function analyzes an NFA (Nondeterministic Finite Automaton) to determine if it represents a simple string length test, optimizing regex matching for patterns that only care about string length.

## Definition


## Detailed Description
This function performs a sophisticated analysis to detect if an NFA represents a 'matchall' pattern - essentially a regex that only tests string length without caring about specific character content (like  which matches any 5-10 character string). When such a pattern is detected, it sets optimization flags and length bounds that allow the regex engine to use faster matching algorithms.

The function performs several validation steps:
1. Checks if the NFA has too many states (> DUPINF * 2) and aborts if so
2. Verifies that all arcs are PLAIN RAINBOW arcs (matching any character) or valid pseudocolor arcs (BOS/BOL/EOS/EOL)
3. Validates that pseudocolor arcs properly replicate RAINBOW arcs at pre/post states
4. Uses recursive analysis to find all possible path lengths through the NFA
5. Ensures path lengths form a consecutive range (no gaps)

If all conditions are met, the function sets nfa->minmatchall, nfa->maxmatchall, and the MATCHALL flag, enabling significant optimization in the regex execution engine.

## Parameters / Member Variables
- : Pointer to the NFA structure to analyze for matchall optimization

## Dependencies
- Functions called/Symbols referenced:
  - DUPINF (maximum duplication count constant)
  - PLAIN, RAINBOW, PSEUDO (arc type/color constants)
  - [check_out_colors_match](check_out_colors_match.md) (validates outgoing arc color consistency)
  - [check_in_colors_match](check_in_colors_match.md) (validates incoming arc color consistency)
  - MALLOC (memory allocation)
  - [checkmatchall_recurse](checkmatchall_recurse.md) (recursive path analysis)
  - MATCHALL (optimization flag constant)
  - FREE (memory deallocation)
- Called from (representative examples):
  - analyze (src/backend/regex/regc_nfa.c:3064)
  - REPLACEARC macro (src/backend/regex/regcomp.c:225)

## Notes and Other Information
- This is a static function, only accessible within the regc_nfa.c file
- The function implements a complex optimization for regex patterns that only test string length
- Uses dynamic memory allocation for path analysis arrays with proper cleanup
- Handles edge cases like multi-state loops and paths exceeding DUPINF length
- Critical for performance optimization in PostgreSQL's regex engine when dealing with length-only patterns
- The optimization can significantly speed up matching for patterns like  or  with length constraints
- Pseudocolor arcs (BOS/BOL/EOS/EOL) are carefully validated to ensure they don't introduce character-specific constraints