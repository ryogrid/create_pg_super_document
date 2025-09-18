# initialize

## Location
[src/backend/regex/rege_dfa.c:731-776](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L731-L776)

## Overview
The  function prepares the initial state set for DFA execution in PostgreSQL's regular expression engine, either by reusing an existing starter state or creating a new one.

## Definition


## Detailed Description
This function sets up the initial state set needed to begin DFA-based regular expression matching. It implements an optimization by checking if a previous starter state set is still available and valid for reuse. If not, it creates a new state set from scratch.

The function performs several key operations:
1. Checks if an existing starter state set can be reused (performance optimization)
2. If needed, allocates a new vacant state set using 
3. Initializes the state set with the pre-state from the compiled NFA
4. Computes and stores the hash value for the state set
5. Sets appropriate flags (STARTER, LOCKED, NOPROGRESS)
6. Resets the "last seen" tracking for all state sets
7. Updates DFA's tracking pointers

## Parameters / Member Variables
- : Pointer to the variables structure containing execution context
- : Pointer to the DFA structure being initialized
- : Pointer to the starting character position in the input string

## Dependencies
- Functions called/Symbols referenced:
  - [getvacant](../g/getvacant.md) (allocates a new state set)
  - BSET (macro to set a bit in the state bitvector)
  - [HASH](../H/HASH.md) (macro to compute hash of state bitvector)
  - STARTER, LOCKED, NOPROGRESS (flag constants)
  - [sset](../s/sset.md), dfa, chr, cnfa (struct types)
- Called from (representative examples):
  - [longest](../l/longest.md) (longest match function)
  - [shortest](../s/shortest.md) (shortest match function)
  - [matchuntil](../m/matchuntil.md) (match until function)
  - LOFF (regex execution function)

## Notes and Other Information
- This is a static function, only accessible within the rege_dfa.c compilation unit
- The function implements an important optimization by reusing existing starter states when possible
- Sets the pre-state as the initial active state in the DFA
- The "maybe untrue, but harmless" comment refers to the lastseen assignment being conservative
- Part of the critical path for regex matching performance in PostgreSQL
- Handles both fresh initialization and reinitialization scenarios