# cfind

## Location
[src/backend/regex/regexec.c:509-548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L509-L548)

## Overview
Finds a match for the main NFA when complications such as backreferences are present.

## Definition
```c
static int
cfind(struct vars *v, struct cnfa *cnfa, struct colormap *cm)
```

## Detailed Description
The `cfind` function handles regex pattern matching when complications are present, such as backreferences, complex constraints, or other advanced regex features that require more sophisticated matching algorithms. It serves as a wrapper that sets up the necessary DFA structures and delegates the core matching logic to `cfindloop`. The function manages two DFAs simultaneously: a search DFA for finding potential match ranges and a main DFA for detailed matching. It handles cleanup and error reporting, and supports the REG_EXPECT flag for extended match information.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing execution state and match results
- `cnfa`: Pointer to the compiled NFA (Non-deterministic Finite Automaton)
- `cm`: Pointer to the color map for character classification

## Dependencies
- Functions called/Symbols referenced:
  - newdfa
  - cfindloop
  - freedfa
  - NOERR
  - OFF
- Called from (representative examples):
  - LOCALDFA execution path when complications are present

## Notes and Other Information
- Uses two separate DFAs (v->dfa1 and v->dfa2) for search and main matching
- Delegates core matching logic to cfindloop function
- Handles REG_EXPECT flag by setting cold start information in v->details
- Performs proper cleanup of DFA resources regardless of success or failure
- Returns the result from cfindloop after handling extended match information
- The function is static and part of the regex execution engine for complex cases