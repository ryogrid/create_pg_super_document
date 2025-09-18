# pg_reg_getfinalstate

## Location
[src/backend/regex/regexport.c:64-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexport.c#L64-L92)

## Overview
Returns the final state identifier of the NFA (Non-deterministic Finite Automaton) within a compiled regular expression.

## Definition
```c
int pg_reg_getfinalstate(const regex_t *regex)
```

## Detailed Description
This function retrieves the final state of the NFA from a compiled regular expression. The final state represents the accepting state where a successful regex match concludes. It accesses the internal compiled NFA structure to obtain the `post` field, which represents the final state identifier. The function includes validation to ensure the regex structure is valid before accessing internal data structures.

## Parameters / Member Variables
- `regex`: A pointer to a compiled regular expression structure (`regex_t`) from which to extract the final state

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
- Returns the `post` field from the compiled NFA structure, which identifies the final/accepting state
- This is part of the regex export API that provides access to internal regex structures for analysis and debugging purposes
- The final state is crucial for regex matching algorithms as it defines the successful termination point for pattern matching
- Together with the initial state, this defines the bounds of the NFA state machine