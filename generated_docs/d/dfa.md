# dfa

## Location
src/backend/regex/regexec.c: 63 - 86

## Overview
The dfa struct represents the core Deterministic Finite Automaton structure in PostgreSQL's regular expression engine, containing all the state management, caching, and execution context needed for efficient pattern matching.

## Definition


## Detailed Description
The dfa structure is the central data structure for PostgreSQL's regular expression DFA implementation. It maintains a complete execution context for pattern matching, including a cache of computed states (ssets), memory areas for bitvector operations, arc storage for state transitions, and references to the compiled NFA and color mapping. The structure supports both forward and backward matching, handles backref processing, and includes optimization features like state caching and memory management flags.

## Parameters / Member Variables
- : Maximum size of the state-set cache
- : Current number of occupied cache entries  
- : Total number of states in the automaton
- : Number of character classes/colors, determines vector lengths
- : Number of unsigned integers needed for state bitvectors
- : Cache array of computed state sets for performance
- : Memory pool for bitvector storage
- : Working memory pointer within statesarea for computations
- : Storage for outgoing arc vectors from each state
- : Storage for incoming arc chains to each state
- : Pointer to the compiled NFA structure
- : Pointer to the character-to-color mapping
- : Text position of last successful match with cache flush
- : Text position of last NOPROGRESS event with cache flush
- : Memory for replacement search operations
- : Backref subexpression number (if this DFA handles backrefs)
- : Minimum repetitions required for backref matching
- : Maximum repetitions allowed for backref matching
- : Flag indicating if the dfa struct itself should be freed
- : Flag indicating if subsidiary arrays should be freed

## Dependencies
- Functions called/Symbols referenced:
  - [sset](../s/sset.md) (state set structure)
  - [arcp](../a/arcp.md) (arc pointer structure)
  - [cnfa](../c/cnfa.md) (compiled NFA structure)
  - [colormap](../c/colormap.md) (character color mapping)
  - [chr](../c/chr.md) (character type)
- Called from (representative examples):
  - [longest](../l/longest.md) (longest match algorithm)
  - [shortest](../s/shortest.md) (shortest match algorithm)
  - [matchuntil](../m/matchuntil.md) (conditional matching)
  - [newdfa](../n/newdfa.md) (DFA construction)
  - [freedfa](../f/freedfa.md) (DFA cleanup)
  - [initialize](../i/initialize.md) (state initialization)
  - [miss](../m/miss.md) (cache miss handling)
  - [smalldfa](../s/smalldfa.md) (small DFA operations)
  - find (pattern search)
  - cfind (case-sensitive search)
  - cfindloop (loop-optimized search)

## Notes and Other Information
The dfa structure is designed for both performance and flexibility. The state cache (ssets) provides significant performance improvements by avoiding recomputation of frequently accessed states. The separation of memory management flags (ismalloced, arraysmalloced) allows for flexible memory allocation strategies, including stack-based allocation for small DFAs. The backref-related fields enable support for backreference matching, which requires special DFA handling. The structure supports both lazy and eager state computation strategies depending on the matching context.