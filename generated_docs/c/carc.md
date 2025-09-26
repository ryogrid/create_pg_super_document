# carc

## Location
[src/include/regex/regguts.h:400-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/regex/regguts.h#L400-L405)

## Overview
The `carc` structure represents a compacted arc in PostgreSQL's regular expression engine, designed to minimize memory usage in compiled NFAs by storing only essential transition information.

## Definition
```c
struct carc
{
    color       co;             /* COLORLESS is list terminator */
    int         to;             /* next-state number */
};
```

## Detailed Description
The `carc` (compacted arc) structure is a space-optimized version of the regular `arc` structure used in compiled NFAs (cNFAs). It stores only the essential information needed for transitions: the color (character class) that triggers the transition and the destination state number. The structure serves dual purposes - representing both plain arcs for character transitions and LACON (Look-Ahead/Look-Behind Constraint) arcs for advanced regex features. Arrays of carc structures are used to represent the outgoing transitions from each state, terminated by a sentinel carc with co == COLORLESS.

## Parameters / Member Variables
- `co`: Color value representing the character class or constraint that triggers this transition. Can be:
  - A regular color number for plain character transitions
  - RAINBOW (negative value) for wildcard transitions that accept any character
  - A LACON constraint number (>= cnfa.ncolors) for lookaround constraints
  - COLORLESS to mark the end of a carc array
- `to`: The destination state number for this transition

## Dependencies
- Functions called/Symbols referenced:
  - `color` (type for the co field)
  - `COLORLESS` (constant for array termination)
- Called from (representative examples):
  - `[compact](compact.md)` (converts NFA to compacted form using carc structures)
  - `[carcsort](carcsort.md)` (sorts carc arrays for optimization)
  - `[carc_cmp](carc_cmp.md)` (comparison function for sorting carcs)
  - `[dumpcstate](../d/dumpcstate.md)` (debugging function to display compacted states)
  - `[miss](../m/miss.md)` (DFA execution function that processes carc transitions)
  - `[traverse_lacons](../t/traverse_lacons.md)` (processes LACON constraints in carcs)

## Notes and Other Information
- Part of PostgreSQL's regex engine located in src/include/regex/regguts.h
- Designed specifically for memory efficiency in compiled NFAs versus the larger `arc` structure used during compilation
- Supports both plain character transitions and lookaround constraints (LAcons) through the co field encoding
- Arrays of carc structures are terminated by a sentinel with co == COLORLESS
- LACON arcs are distinguished by having co >= cnfa.ncolors
- The RAINBOW color (negative value) represents wildcard transitions
- Used extensively in the DFA execution engine for efficient pattern matching
- The compacted representation enables faster execution and reduced memory footprint for regex matching