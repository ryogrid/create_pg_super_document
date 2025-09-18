# compact

## Location
[src/backend/regex/regc_nfa.c:3514-3604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L3514-L3604)

## Overview
Constructs the compact representation of an NFA (Non-deterministic Finite Automaton) by converting it into a CNFA (Compact NFA) structure for efficient runtime execution.

## Definition


## Detailed Description
This function transforms a regular NFA into a compact representation optimized for execution. The compact form uses arrays instead of linked lists for better cache locality and faster traversal during pattern matching. The conversion process involves:

1. **Memory allocation**: Allocates arrays for state flags, state pointers, and arc storage
2. **State mapping**: Maps NFA states to compact array indices
3. **Arc conversion**: Converts linked arc lists to contiguous arrays, handling both PLAIN and LACON (lookahead/lookbehind assertion) arc types
4. **Sorting**: Sorts arcs within each state using  for efficient searching
5. **Metadata transfer**: Copies essential NFA properties like pre/post states, BOS/EOS markers, colors, and flags
6. **No-progress marking**: Identifies states that don't advance the input position

The function handles memory allocation failures gracefully and ensures all arc arrays are properly terminated with COLORLESS endmarkers.

## Parameters / Member Variables
- : Source NFA structure to be converted
- : Target compact NFA structure to be populated

## Dependencies
- Functions called/Symbols referenced:
  -  (error checking macro)
  - ,  (memory management)
  -  (color management)
  -  (arc sorting)
  -  (error reporting)
  - Constants: , , , , , , 
- Called from (representative examples):
  -  (at src/backend/regex/regcomp.c:2381)

## Notes and Other Information
- Critical for PostgreSQL's regex engine performance optimization
- Converts dynamic linked structures to static arrays for better cache performance
- Handles both regular arcs (PLAIN) and lookaround assertion arcs (LACON)
- LACON arcs use colors beyond the normal color range (ncolors + lacon_id)
- Each state's arc array is terminated with a COLORLESS endmarker
- No-progress states are specially marked to prevent infinite loops in matching
- Memory allocation failure results in REG_ESPACE error
- Arc sorting within states enables binary search during execution