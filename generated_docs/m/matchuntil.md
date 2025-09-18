# matchuntil

## Location
src/backend/regex/rege_dfa.c: 371 - 505

## Overview
Implements an incremental matching engine for search-style NFAs that determines match existence with O(N) time complexity across multiple calls.

## Definition
```c
static int
matchuntil(struct vars *v,
           struct dfa *d,
           chr *probe,          /* we want to know if a match ends here */
           struct sset **lastcss, /* state storage across calls */
           chr **lastcp)        /* state storage across calls */
```

## Detailed Description
The `matchuntil` function is designed for incremental regex matching with search-style NFAs (patterns that behave as if they had a leading `.*`). It efficiently determines whether a match exists starting at v->start and ending at the probe position. The key innovation is that multiple calls with non-decreasing probe values require only O(N) time total, not O(N²), by maintaining state between calls. This makes it highly efficient for scanning operations where you need to check match endings at multiple positions.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex execution context and state
- `d`: Pointer to DFA structure containing the compiled search automaton
- `probe`: Target ending position to test for match completion
- `lastcss`: Pointer to state set storage that persists across multiple calls
- `lastcp`: Pointer to character position storage that persists across multiple calls

## Dependencies
- Functions called/Symbols referenced:
  - initialize (for setting up initial DFA state when needed)
  - miss (for handling DFA state transitions)
  - GETCOLOR (for character-to-color mapping)
  - FDEBUG (for debug tracing)
  - MATCHALL, DUPINF (for MATCHALL NFA optimization)
- Called from (representative examples):
  - lacon (lookahead/lookbehind constraint processing)
  - LOFF (regex execution offset function)

## Notes and Other Information
- Optimized for search patterns with leading .* behavior
- Maintains persistent state between calls for O(N) amortized performance
- Includes fast path for MATCHALL NFAs with direct character counting
- Supports both normal and traced execution modes
- Returns 1 for match found, 0 for no match or internal error
- Critical for efficient implementation of lookahead/lookbehind assertions in PostgreSQL regex engine