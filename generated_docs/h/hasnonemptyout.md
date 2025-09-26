# hasnonemptyout

## Location
[src/backend/regex/regc_nfa.c:575-591](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L575-L591)

## Overview
Checks whether a state in a regular expression NFA has at least one outgoing arc that is not an EMPTY arc.

## Definition
```c
static int hasnonemptyout(struct state *s)
```

## Detailed Description
The `hasnonemptyout` function traverses the outgoing arcs of a given state to determine if any of them are non-EMPTY arcs. In regular expression NFAs, EMPTY arcs represent epsilon transitions that can be traversed without consuming any input characters. This function is used to identify states that have meaningful outgoing transitions (non-epsilon transitions) that actually consume input.

The function iterates through the state's outgoing arc chain (`outs`) and checks each arc's type. If it finds any arc whose type is not EMPTY, it immediately returns 1 (true). If all outgoing arcs are EMPTY or there are no outgoing arcs, it returns 0 (false).

## Parameters / Member Variables
- `s`: The state to examine for non-empty outgoing arcs

## Dependencies
- Functions called/Symbols referenced:
  - struct arc (data structure)
  - struct state (data structure)
  - EMPTY (arc type constant)
- Called from (representative examples):
  - [fixempties](../f/fixempties.md) (in regc_nfa.c:2217)

## Notes and Other Information
- This is a static function internal to the regex NFA construction module
- Returns 1 if the state has at least one non-EMPTY outgoing arc, 0 otherwise
- Used during NFA optimization, particularly in the process of eliminating empty transitions
- Part of PostgreSQL's internal regular expression engine implementation
- The function performs a simple linear search through the outgoing arc chain