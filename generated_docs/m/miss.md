# miss

## Location
[src/backend/regex/rege_dfa.c:777-915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L777-L915)

## Overview
The  function handles stateset cache misses in PostgreSQL's regular expression DFA engine by computing the next stateset when transitioning on a given character.

## Definition

```c
static struct sset *
miss(struct vars *v,
	 struct dfa *d,
	 struct sset *css,
	 color co,
	 chr *cp,					/* next chr */
	 chr *start)				/* where the attempt got started */
```
## Detailed Description
This function is the core workhorse of DFA state transitions. When a cached stateset transition is not available (a "cache miss"), this function computes what stateset would result from consuming a specific character from the current stateset.

The function performs several complex operations:

1. **Cache Check**: First verifies if this is actually a miss (optimization for false cache misses)
2. **State Computation**: Builds a new stateset by following PLAIN arcs that consume the input character
3. **LACON Handling**: Processes Look-Ahead CONstraints (LACONs) through transitive closure
4. **Cache Lookup**: Checks if the computed stateset already exists in the cache
5. **Cache Entry Creation**: Creates a new cache entry if needed
6. **Link Creation**: Links the transition unless LACONs were involved (to force recomputation)

The function handles both regular character transitions and complex lookahead constraints, making it essential for correct regex matching behavior.

## Parameters / Member Variables
- : Pointer to the variables structure containing execution context and error handling
- : Pointer to the DFA structure containing state and cache information
- : Pointer to the current stateset from which we're transitioning
- : The color (character class) of the input character being consumed
- : Pointer to the character after the current one (used for LACON testing)
- : Pointer to the start of the input string (used for cache replacement heuristics)

## Dependencies
- Functions called/Symbols referenced:
  - [getvacant](../g/getvacant.md) (allocates new stateset entries)
  - [lacon](../l/lacon.md) (tests Look-Ahead CONstraints)
  - BSET, ISBSET (bitvector manipulation macros)
  - [HASH](../H/HASH.md), HIT (hashing and cache lookup macros)
  - FDEBUG (debugging output macro)
  - INTERRUPT (operation cancellation check)
  - Various flag constants (PSEUDO, RAINBOW, COLORLESS, HASLACONS, etc.)
- Called from (representative examples):
  - [longest](../l/longest.md) (longest match function)
  - [shortest](../s/shortest.md) (shortest match function)
  - [matchuntil](matchuntil.md) (match until function)
  - LOFF (regex execution function)

## Notes and Other Information
- This is a static function, only accessible within the rege_dfa.c compilation unit
- Performance critical: includes operation cancellation checks as a compromise between responsiveness and speed
- Handles complex LACON (lookahead constraint) logic that may prevent caching of transitions
- Returns NULL for match failures or internal errors (with v->err set for errors)
- The transitive closure loop handles LACONs that may enable further LACONs
- Cache linking is conditionally skipped when LACONs are involved to ensure correct recomputation
- Part of the most performance-sensitive code path in PostgreSQL's regex engine

## Simplified Source
```c
static struct sset *miss(struct vars *v, struct dfa *d, struct sset *css,
                        color co, chr *cp, chr *start)
{
    struct cnfa *cnfa = d->cnfa;
    int i;
    unsigned h;
    struct carc *ca;
    struct sset *p;
    int ispseudocolor, ispost, noprogress, gotstate;
    int dolacons, sawlacons;

    // Check if it's actually a cache hit (optimization)
    if (css->outs[co] != NULL) {
        return css->outs[co];
    }

    // Check for operation cancellation
    INTERRUPT(v->re);

    // Build new stateset by following PLAIN arcs
    for (i = 0; i < d->wordsper; i++)
        d->work[i] = 0;

    ispseudocolor = d->cm->cd[co].flags & PSEUDO;
    ispost = 0;
    noprogress = 1;
    gotstate = 0;

    // Follow arcs that consume the input character
    for (i = 0; i < d->nstates; i++) {
        if (ISBSET(css->states, i)) {
            for (ca = cnfa->states[i]; ca->co != COLORLESS; ca++) {
                if (ca->co == co || (ca->co == RAINBOW && !ispseudocolor)) {
                    BSET(d->work, ca->to);
                    gotstate = 1;
                    if (ca->to == cnfa->post)
                        ispost = 1;
                    if (!(cnfa->stflags[ca->to] & CNFA_NOPROGRESS))
                        noprogress = 0;
                }
            }
        }
    }

    if (!gotstate)
        return NULL;  // No valid transitions

    // Handle LACON (Look-Ahead CONstraints) transitive closure
    dolacons = (cnfa->flags & HASLACONS);
    sawlacons = 0;
    while (dolacons) {
        dolacons = 0;
        for (i = 0; i < d->nstates; i++) {
            if (ISBSET(d->work, i)) {
                for (ca = cnfa->states[i]; ca->co != COLORLESS; ca++) {
                    if (ca->co < cnfa->ncolors)
                        continue;  // Not a LACON arc
                    if (ISBSET(d->work, ca->to))
                        continue;  // Already processed

                    sawlacons = 1;
                    if (!lacon(v, cnfa, cp, ca->co)) {
                        if (ISERR())
                            return NULL;
                        continue;  // LACON failed
                    }
                    if (ISERR())
                        return NULL;

                    BSET(d->work, ca->to);
                    dolacons = 1;  // May enable more LACONs
                    if (ca->to == cnfa->post)
                        ispost = 1;
                    if (!(cnfa->stflags[ca->to] & CNFA_NOPROGRESS))
                        noprogress = 0;
                }
            }
        }
    }

    h = HASH(d->work, d->wordsper);

    // Check if this stateset already exists in cache
    for (p = d->ssets, i = d->nssused; i > 0; p++, i--) {
        if (HIT(h, d->work, p, d->wordsper))
            break;
    }

    if (i == 0) {
        // Need new cache entry
        p = getvacant(v, d, cp, start);
        if (p == NULL)
            return NULL;

        // Copy computed stateset
        for (i = 0; i < d->wordsper; i++)
            p->states[i] = d->work[i];
        p->hash = h;
        p->flags = (ispost) ? POSTSTATE : 0;
        if (noprogress)
            p->flags |= NOPROGRESS;
    }

    // Link transition unless LACONs were involved
    if (!sawlacons) {
        css->outs[co] = p;
        css->inchain[co] = p->ins;
        p->ins.ss = css;
        p->ins.co = co;
    }

    return p;
}
```

This function handles DFA state transitions by:
1. Checking for false cache misses (optimization)
2. Computing reachable states via character consumption
3. Processing lookahead constraints (LACONs) through transitive closure
4. Checking for existing cached statesets
5. Creating new cache entries when needed
6. Linking transitions (except when LACONs require recomputation)