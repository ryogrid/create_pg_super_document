# smalldfa

## Location
src/backend/regex/regexec.c: 92 - 100

## Overview
The smalldfa struct is a stack-allocated, fixed-size variant of the DFA structure optimized for small regular expressions with limited states and colors, avoiding heap allocation overhead.

## Definition


## Detailed Description
The smalldfa structure is a performance optimization for PostgreSQL's regular expression engine that embeds all necessary DFA components in a single stack-allocatable structure. It contains a base dfa struct followed by pre-allocated arrays for state sets, bitvector storage, outgoing arcs, and incoming arcs. This design eliminates dynamic memory allocation overhead for simple regular expressions that fit within the predefined size limits (FEWSTATES and FEWCOLORS), providing faster initialization and cleanup.

## Parameters / Member Variables
- : Base DFA structure that must be positioned first for type casting compatibility
- : Fixed-size array of state sets, sized for FEWSTATES * 2 entries
- : Pre-allocated bitvector storage area, sized for FEWSTATES * 2 + WORK words
- : Fixed-size array of outgoing arc pointers, sized for FEWSTATES * 2 * FEWCOLORS entries  
- : Fixed-size array of incoming arc structures, sized for FEWSTATES * 2 * FEWCOLORS entries

## Dependencies
- Functions called/Symbols referenced:
  - dfa (base DFA structure)
  - sset (state set structure)
  - arcp (arc pointer structure)
  - FEWSTATES (constant defining small state limit)
  - FEWCOLORS (constant defining small color limit)
  - WORK (constant defining work area size)
- Called from (representative examples):
  - newdfa (DFA construction and size determination)
  - DOMALLOC (memory allocation decision macro)
  - vars (variable initialization)
  - LOFF (offset calculations)

## Notes and Other Information
The smalldfa structure demonstrates a common PostgreSQL optimization pattern where small, frequent operations use stack allocation to avoid malloc/free overhead. The dfa member must be first to enable safe casting between smalldfa* and dfa* pointers. This structure is automatically selected by the DFA construction code when the regular expression complexity fits within the FEWSTATES and FEWCOLORS limits, providing significant performance benefits for simple patterns commonly found in database queries.