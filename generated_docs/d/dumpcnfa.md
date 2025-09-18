# dumpcnfa

## Location
[src/backend/regex/regc_nfa.c:3823-3859](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L3823-L3859)

## Overview
A debugging function that prints a human-readable representation of a compiled NFA (Non-deterministic Finite Automaton) structure to a file stream.

## Definition


## Detailed Description
The  function outputs comprehensive debugging information about a compiled NFA structure. It first prints header information including pre and post states, beginning/end of string and line anchors, and various flags. Then it iterates through all states in the NFA, calling  to print detailed information about each individual state. This function is only available when compiled with REG_DEBUG enabled and serves as a crucial debugging tool for regular expression compilation and optimization.

The function formats the output to show:
- Pre and post state numbers
- Beginning of string (bos) and beginning of line (bol) color information 
- End of string (eos) and end of line (eol) color information
- Special flags like HASLACONS (has lookahead/lookbehind constraints)
- MATCHALL flag with minimum and maximum match counts
- Detailed state-by-state breakdown via dumpcstate calls

## Parameters / Member Variables
- : Pointer to the compiled NFA structure to be dumped
- : File stream where the debug output will be written

## Dependencies
- Functions called/Symbols referenced:
  - [dumpcstate](dumpcstate.md)
  - fprintf
  - fflush
  - COLORLESS
  - HASLACONS
  - MATCHALL
  - DUPINF
- Called from (representative examples):
  - REPLACEARC (in regcomp.c:240)
  - [dump](dump.md) (in regcomp.c:2521, 2547)
  - [stdump](../s/stdump.md) (in regcomp.c:2619)

## Notes and Other Information
- This function is only compiled when REG_DEBUG is defined
- Located in src/backend/regex/regc_nfa.c:3823-3859
- Part of PostgreSQL's internal regular expression engine debugging infrastructure
- Works in conjunction with dumpcstate to provide complete NFA state machine visualization
- The output format uses specific conventions: states followed by ':' indicate CNFA_NOPROGRESS flag, while '.' indicates normal states
- DUPINF represents infinite repetition in match-all scenarios