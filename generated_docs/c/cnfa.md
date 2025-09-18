# cnfa

## Location
src/include/regex/regguts.h: 406 - 410

## Overview
The  struct represents a compiled NFA (Non-deterministic Finite Automaton) used in PostgreSQL's regular expression engine for efficient pattern matching.

## Definition


## Detailed Description
The  struct is the core data structure representing a compiled NFA in PostgreSQL's regex engine. It stores the complete state machine representation of a regular expression pattern after compilation. The structure includes state information, transition arcs between states, color mappings for character classes, and various optimization flags. This compiled form enables efficient pattern matching during regex execution by providing a direct representation of the automaton's transitions and states.

## Parameters / Member Variables
- : Total number of states in the NFA
- : Number of distinct colors used for character classification (maximum color value + 1)
- : Bitmask containing behavioral flags:
  - : Indicates the NFA uses lookaround constraints (lookahead/lookbehind)
  - : Indicates the NFA matches all strings within a specific length range
  - : Indicates presence of CANTMATCH arcs (used in NFA but not in compiled CNFA)
- : State number for the initial setup state
- : State number for the final teardown state
- : Colors assigned to Beginning-Of-String (BOS) and Beginning-Of-Line (BOL) anchors
- : Colors assigned to End-Of-String (EOS) and End-Of-Line (EOL) anchors
- : Byte array containing per-state flags (e.g., CNFA_NOPROGRESS for no-progress states)
- : Array of pointers to outgoing arc lists for each state
- : Memory area containing all transition arcs in a single allocation
- : Minimum character count to match (used only for MATCHALL NFAs, -1 otherwise)
- : Maximum character count to match or DUPINF for unlimited (used only for MATCHALL NFAs, -1 otherwise)

## Dependencies
- Functions called/Symbols referenced:
  - struct carc (for transition arcs)
  - color (for character color classification)
- Called from (representative examples):
  - [compact](compact.md) (NFA compilation)
  - [freecnfa](../f/freecnfa.md) (memory cleanup)
  - [dumpcnfa](../d/dumpcnfa.md) (debugging output)
  - [longest](../l/longest.md)/shortest (pattern matching functions)
  - [newdfa](../n/newdfa.md) (DFA construction)
  - [pg_reg_getnumstates](../p/pg_reg_getnumstates.md) (export functions)

## Notes and Other Information
- The CNFA is the compiled, optimized form of an NFA used for actual pattern matching execution
- State transitions are represented through the  (compiled arc) structures pointed to by the  array
- The color system is used to group equivalent characters together for more efficient processing
- The structure supports both standard regex matching and specialized MATCHALL patterns for length-based matching
- Memory is managed efficiently with  containing all transition data in a single allocation and  providing indexed access
- The structure is central to PostgreSQL's regex engine performance, enabling fast pattern matching operations