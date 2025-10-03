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
- `*v`: Pointer to the variables structure containing execution context and cached state
- `*pcnfa`: Pointer to the parent compiled NFA structure
- `*cp`: Pointer to the character position where the constraint is being tested
- `co`: The "color" (identifier) of the lookaround constraint to evaluate
## Dependencies
- Functions called/Symbols referenced:
  - [getladfa](../g/getladfa.md) (retrieves the DFA for the lookaround constraint)
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
  - [getladfa](../g/getladfa.md)
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

## Simplified Source

```c
static int lacon(struct vars *v, struct cnfa *pcnfa, chr *cp, color co)
{
    int n;
    struct subre *sub;
    struct dfa *d;
    chr *end;
    int satisfied;

    // Protect against stack overflow in recursive calls
    if (STACK_TOO_DEEP(v->re)) {
        ERR(REG_ETOOBIG);
        return 0;
    }

    // Extract LACON index from color
    n = co - pcnfa->ncolors;
    assert(n > 0 && n < v->g->nlacons && v->g->lacons != NULL);

    // Get the lookaround constraint and its DFA
    sub = &v->g->lacons[n];
    d = getladfa(v, n);
    if (d == NULL)
        return 0;

    if (LATYPE_IS_AHEAD(sub->latype)) {
        // LOOKAHEAD: test if pattern matches forward from current position
        // Use shortest() instead of longest() for better performance
        end = shortest(v, d, cp, cp, v->stop, (chr **) NULL, (int *) NULL);

        // Positive lookahead: satisfied if match found
        // Negative lookahead: satisfied if no match found
        satisfied = LATYPE_IS_POS(sub->latype) ? (end != NULL) : (end == NULL);
    } else {
        // LOOKBEHIND: test if pattern matches backward
        // Use matchuntil() with caching to avoid O(N²) behavior
        // when repeatedly testing lookbehind in an N-character string
        satisfied = matchuntil(v, d, cp, &v->lblastcss[n], &v->lblastcp[n]);

        // Negative lookbehind: invert the result
        if (!LATYPE_IS_POS(sub->latype))
            satisfied = !satisfied;
    }

    return satisfied;  // 1 if constraint satisfied, 0 if not
}
```