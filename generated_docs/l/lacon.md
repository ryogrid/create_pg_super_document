# lacon

## Location
[src/backend/regex/rege_dfa.c:916-972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L916-L972)

## Overview
The  function is a lookaround-constraint checker that evaluates whether a specific lookahead or lookbehind assertion is satisfied at a given position in the input string.

## Definition

```c
static int						/* predicate:  constraint satisfied? */
lacon(struct vars *v,
	  struct cnfa *pcnfa,		/* parent cnfa */
	  chr *cp,
	  color co)					/* "color" of the lookaround constraint */
```
## Detailed Description
This function implements the evaluation logic for lookaround assertions (lookahead and lookbehind) in PostgreSQL's regular expression engine. LACONs (Look-Around CONstraints) are advanced regex features that allow matching based on what comes before or after a position without consuming characters.

The function performs different logic based on the type of lookaround:

1. **Lookahead**: Uses  to test if the pattern matches forward from the current position
2. **Lookbehind**: Uses  with caching for efficient O(N) performance instead of O(N²)

The function includes important optimizations:
- For lookbehind, it caches DFA state across calls to avoid repeatedly testing the same constraints
- Stack overflow protection for recursive calls
- Efficient handling of both positive and negative assertions

## Parameters / Member Variables
- : Pointer to the variables structure containing execution context and cached state
- : Pointer to the parent compiled NFA structure
- : Pointer to the character position where the constraint is being tested
- : The "color" (identifier) of the lookaround constraint to evaluate

## Dependencies
- Functions called/Symbols referenced:
  - getladfa (retrieves the DFA for the lookaround constraint)
  - [shortest](../s/shortest.md) (tests lookahead constraints by finding shortest match)
  - [matchuntil](../m/matchuntil.md) (tests lookbehind constraints with caching)
  - STACK_TOO_DEEP (stack overflow protection macro)
  - LATYPE_IS_AHEAD, LATYPE_IS_POS (lookaround type checking macros)
  - ERR (error reporting macro)
  - FDEBUG (debugging output macro)
- Called from (representative examples):
  - [miss](../m/miss.md) (during DFA state transition computation)
  - LOFF (regex execution function)

## Dependencies
- Functions called/Symbols referenced:
  - getladfa
  - [shortest](../s/shortest.md)
  - [matchuntil](../m/matchuntil.md)
  - STACK_TOO_DEEP
  - LATYPE_IS_AHEAD
  - LATYPE_IS_POS
  - ERR
  - FDEBUG
- Called from (representative examples):
  - [miss](../m/miss.md)
  - LOFF

## Notes and Other Information
- This is a static function, only accessible within the rege_dfa.c compilation unit
- Returns 1 if the constraint is satisfied, 0 if not satisfied or on error
- Includes recursive call protection to prevent stack overflow
- Implements sophisticated caching for lookbehind assertions to achieve linear time complexity
- The color parameter is actually an offset from the base color count to identify the specific LACON
- Critical for implementing advanced regex features like (?=...), (?!...), (?<=...), (?<!...)
- Performance optimized: uses shortest match for lookahead and cached state for lookbehind