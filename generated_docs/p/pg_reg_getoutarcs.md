# pg_reg_getoutarcs

## Location
[src/backend/regex/regexport.c:155-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexport.c#L155-L173)

## Overview
Extracts the outgoing NFA (Non-deterministic Finite Automaton) arcs from a specified state in a compiled regular expression, writing them to a provided array.

## Definition

```c
struct cnfa *cnfa;
```
## Detailed Description
This function retrieves the outgoing arcs from a specific state in a compiled regular expression's NFA. The function masks the existence of LACON (lookahead constraint) arcs from the caller by automatically traversing them and returning only the reachable regular arcs. This design choice is necessary because the output representation doesn't support arcs that consume no character when traversed.

The function validates the input parameters and uses the internal  helper function to recursively traverse LACON arcs and collect all reachable ordinary arcs. The arcs are written to the provided array up to the specified length limit.

## Parameters / Member Variables
- : Pointer to the compiled regular expression structure containing the NFA
- : State number from which to retrieve outgoing arcs
- : Output array to store the retrieved arcs (must be pre-allocated)
- : Maximum number of arcs that can be stored in the arcs array

## Dependencies
- Functions called/Symbols referenced:
  - : Recursively traverses LACON arcs to find reachable regular arcs
  - : Magic number constant for regex validation
  - : Internal regex structure containing the compiled NFA
  - : Compiled NFA structure
- Called from (representative examples):
  - External code that needs to analyze regex NFA structure

## Notes and Other Information
- The caller must ensure that  is at least as large as the value returned by  to retrieve all available arcs
- If the provided array is too small, only the first  arcs will be returned
- The function handles invalid state numbers gracefully by returning early
- LACON arcs are automatically satisfied and recursively traversed, making them invisible to the caller
- The regex library ensures that LACON arcs never lead directly to the final state