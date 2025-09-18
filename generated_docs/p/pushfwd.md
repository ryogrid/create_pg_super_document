# pushfwd

## Location
src/backend/regex/regc_nfa.c: 1811 - 1890

## Overview
Eliminates forward constraint arcs ($ and AHEAD) by pushing them forward through the NFA structure, ultimately converting them to PLAIN arcs with special boundary colors.

## Definition


## Detailed Description
This function is the forward counterpart to , handling the elimination of forward-looking constraint arcs. The process works iteratively:

1. **Iterative Processing**: Scans all states and their incoming arcs repeatedly until no more progress can be made
2. **Constraint Identification**: Looks for '$' (end-of-line) and AHEAD (lookahead assertion) arcs
3. **Forward Pushing**: Uses the  function to move these constraints forward through the NFA
4. **Intermediate State Management**: Tracks and cleans up temporary intermediate states created during the pushing process
5. **State Cleanup**: Removes states that become useless (no inputs or outputs) after constraint removal
6. **Final Conversion**: Converts any remaining '$' constraints at the post state to PLAIN arcs using special EOS/EOL colors

The function works in tandem with  to ensure that all constraint arcs are either eliminated entirely or moved to positions where they can be converted to regular color-based arcs that the executor can handle.

## Parameters / Member Variables
- : Pointer to the NFA structure being optimized
- : File pointer for debug output; NULL if no debug output desired

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that performs the forward pushing of individual constraints
  - : Removes useless states from the NFA
  - : Creates new arcs in the NFA
  - : Deallocates arc structures
  - : Debug output function (when debug enabled)
  - : Error checking macro
  - : Constant representing lookahead assertion arcs
  - : Constant representing regular character-matching arcs
- Called from (representative examples):
  -  (src/backend/regex/regc_nfa.c:1623)

## Notes and Other Information
- This is a static function, only visible within the regc_nfa.c compilation unit
- Part of the NFA optimization pipeline that prepares regex structures for execution
- Works in conjunction with  to handle complete constraint elimination
- The function uses a progress-driven loop to ensure all possible constraint eliminations are performed
- Successfully pushed '$' constraints are converted to PLAIN arcs using  colors
- Critical for ensuring the final NFA contains only arc types the executor can process
- Handles cleanup of temporary intermediate states created during the pushing process
- Processes incoming arcs () rather than outgoing arcs, complementing 's approach