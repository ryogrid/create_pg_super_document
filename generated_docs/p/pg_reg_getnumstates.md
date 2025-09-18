# pg_reg_getnumstates

## Location
[src/backend/regex/regexport.c:36-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexport.c#L36-L49)

## Overview
Returns the total number of NFA (Non-deterministic Finite Automaton) states in a compiled regular expression.

## Definition
```c
int pg_reg_getnumstates(const regex_t *regex)
```

## Detailed Description
This function extracts and returns the total count of states from the NFA structure within a compiled regular expression. It accesses the internal structure of the regex to obtain the state count from the compiled NFA (Compiled NFA) search structure. The function performs validation to ensure the regex is valid by checking the magic number before accessing internal data structures.

## Parameters / Member Variables
- `regex`: A pointer to a compiled regular expression structure (`regex_t`) from which to extract the state count

## Dependencies
- Functions called/Symbols referenced:
  - `regex_t` (structure type)
  - `REMAGIC` (magic number constant)
  - [guts](../g/guts.md) (internal regex structure)
  - [cnfa](../c/cnfa.md) (compiled NFA structure)
- Called from (representative examples):
  - [regex_arc_t](../r/regex_arc_t.md) (referenced in regexport.h)

## Notes and Other Information
- The function includes an assertion to validate that the regex pointer is not NULL and that the regex has the correct magic number (REMAGIC)
- Accesses the internal `search` field of the compiled NFA structure to retrieve the `nstates` count
- This is part of the regex export API that provides access to internal regex structures for analysis and debugging purposes