# dumpstate

## Location
[src/backend/regex/regc_nfa.c:3695-3720](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L3695-L3720)

## Overview
A debugging function that outputs detailed information about a single NFA state in human-readable format, including its arcs and chain integrity checks.

## Definition


## Detailed Description
The  function provides a detailed textual representation of an individual NFA state for debugging purposes. It displays the state number, temporary marker status, and state flag, along with validation of the state's chain integrity. The function also outputs information about outgoing arcs by calling , and performs sanity checks on incoming arc chains to ensure data structure consistency.

This function is part of the debugging infrastructure (compiled only when  is defined) and helps developers understand the internal structure of NFA states during regex compilation and optimization processes.

## Parameters / Member Variables
- : Pointer to the NFA state structure to be dumped
- : File stream where the dump output will be written

## Dependencies
- Functions called/Symbols referenced:
  - [dumparcs](dumparcs.md) (for dumping outgoing arcs from this state)
  - [arc](../a/arc.md) (struct type for representing transitions)
  - [flag](../f/flag.md) (state flag indicating special properties)
- Called from (representative examples):
  - [dumpnfa](dumpnfa.md) (src/backend/regex/regc_nfa.c:3678)
  - REPLACEARC (src/backend/regex/regcomp.c:237)

## Notes and Other Information
- Only available in debug builds (wrapped in )
- Performs chain integrity validation by checking if 
- Displays 'T' marker if the state has a temporary marker ()
- Shows the state flag character or '.' if no flag is set
- Validates incoming arc chains to detect data structure corruption
- Reports states with no outgoing arcs, which may indicate terminal states or structural issues
- Calls  to ensure immediate output to the file stream
- Part of PostgreSQL's regex engine debugging infrastructure for analyzing state machine structure