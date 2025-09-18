# pg_reg_getinitialstate

## Location
[src/backend/regex/regexport.c:50-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexport.c#L50-L63)

## Overview
Returns the initial state identifier of the NFA (Non-deterministic Finite Automaton) within a compiled regular expression.

## Definition
```c
int pg_reg_getinitialstate(const regex_t *regex)
```

## Detailed Description
This function retrieves the initial state of the NFA from a compiled regular expression. The initial state is the starting point for regex matching operations. It accesses the internal compiled NFA structure to obtain the `pre` field, which represents the initial state identifier. Like other regex export functions, it includes validation to ensure the regex structure is valid before accessing internal data.

## Parameters / Member Variables
- `regex`: A pointer to a compiled regular expression structure (`regex_t`) from which to extract the initial state

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
- Returns the `pre` field from the compiled NFA structure, which identifies the initial state
- This is part of the regex export API that provides access to internal regex structures for analysis and debugging purposes
- The initial state is crucial for regex matching algorithms as it defines the starting point for state transitions