# shortest

## Location
[src/backend/regex/rege_dfa.c:204-370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L204-L370)

## Overview
Implements the shortest-preferred matching engine for DFA-based regular expression matching in PostgreSQL.

## Definition
```c
static chr *
shortest(struct vars *v,
         struct dfa *d,
         chr *start,       /* where the match should start */
         chr *min,         /* match must end at or after here */
         chr *max,         /* match must end at or before here */
         chr **coldp,      /* store coldstart pointer here, if non-NULL */
         int *hitstopp)    /* record whether hit v->stop, if non-NULL */
```

## Detailed Description
The `shortest` function implements shortest-preferred matching for DFA-based regex execution. Unlike `longest`, it stops as soon as it finds the first valid match within the specified range. It processes text character by character while maintaining DFA state sets, but breaks out of the scanning loop immediately when a POSTSTATE (accepting state) is reached and the minimum match length is satisfied. The function includes optimizations for backref patterns and MATCHALL NFAs, and provides coldstart information for optimization purposes.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex execution context and state
- `d`: Pointer to DFA structure containing the compiled automaton
- `start`: Starting character position where matching should begin
- `min`: Minimum ending position - match must end at or after this point
- `max`: Maximum ending position - match must end at or before this point
- `coldp`: Optional pointer to store the last no-progress state set location
- `hitstopp`: Optional pointer to record whether matching hit the global stop position

## Dependencies
- Functions called/Symbols referenced:
  - [dfa_backref](../d/dfa_backref.md) (for handling backreferences)
  - [initialize](../i/initialize.md) (for setting up initial DFA state)
  - [miss](../m/miss.md) (for handling state transitions)
  - lastcold (for coldstart optimization)
  - GETCOLOR (for character-to-color mapping)
  - FDEBUG (for debug tracing)
- Called from (representative examples):
  - [dfa_backref](../d/dfa_backref.md) (recursive calls for backref processing)
  - [lacon](../l/lacon.md) (lookahead/lookbehind processing)
  - LOFF (regex execution offset function)
  - [find](../f/find.md), cfindloop (main search functions)

## Notes and Other Information
- Prioritizes finding the shortest valid match rather than the longest
- Contains early termination logic when POSTSTATE is reached within bounds
- Handles complex boundary conditions between min/max positions
- Supports both coldstart optimization and hitStop tracking
- Critical for non-greedy quantifiers and minimal matching patterns in PostgreSQL regex engine

## Simplified Source

```c
static chr *shortest(struct vars *v, struct dfa *d, chr *start, chr *min, chr *max,
                    chr **coldp, int *hitstopp)
{
    chr *cp;
    chr *realmin = (min == v->stop) ? min : min + 1;
    chr *realmax = (max == v->stop) ? max : max + 1;
    color co;
    struct sset *css;
    struct sset *ss;
    struct colormap *cm = d->cm;

    // Initialize output parameters
    if (coldp != NULL)
        *coldp = NULL;
    if (hitstopp != NULL)
        *hitstopp = 0;

    // Fast path: handle backreferences to known strings
    if (d->backno >= 0) {
        assert((size_t) d->backno < v->nmatch);
        if (v->pmatch[d->backno].rm_so >= 0) {
            cp = dfa_backref(v, d, start, min, max, true);
            if (cp != NULL && coldp != NULL)
                *coldp = start;
            return cp;
        }
    }

    // Fast path: handle MATCHALL NFAs (patterns that match any sequence)
    if (d->cnfa->flags & MATCHALL) {
        size_t nchr = min - start;

        // Check bounds against min/max match lengths
        if (d->cnfa->maxmatchall != DUPINF && nchr > d->cnfa->maxmatchall)
            return NULL;
        if ((max - start) < d->cnfa->minmatchall)
            return NULL;
        if (nchr < d->cnfa->minmatchall)
            min = start + d->cnfa->minmatchall;

        if (coldp != NULL)
            *coldp = start;
        return min;
    }

    // Initialize DFA state
    css = initialize(v, d, start);
    if (css == NULL)
        return NULL;
    cp = start;

    // Handle startup character/boundary
    if (cp == v->start) {
        co = d->cnfa->bos[(v->eflags & REG_NOTBOL) ? 0 : 1];  // Beginning of string
    } else {
        co = GETCOLOR(cm, *(cp - 1));  // Get color of previous character
    }

    css = miss(v, d, css, co, cp, start);
    if (css == NULL)
        return NULL;
    css->lastseen = cp;
    ss = css;

    // Main scanning loop - process each character
    while (cp < realmax) {
        co = GETCOLOR(cm, *cp);  // Get color of current character
        ss = css->outs[co];      // Get next state

        if (ss == NULL) {
            // Handle state transition miss
            ss = miss(v, d, css, co, cp + 1, start);
            if (ss == NULL)
                break;  // No valid transition
        }

        cp++;
        ss->lastseen = cp;
        css = ss;

        // SHORTEST-PREFERRED: exit as soon as we have a valid match
        if ((ss->flags & POSTSTATE) && cp >= realmin)
            break;  // Found shortest match!
    }

    if (ss == NULL)
        return NULL;

    // Report coldstart information for optimization
    if (coldp != NULL)
        *coldp = lastcold(v, d);

    // Handle end-of-match positioning
    if ((ss->flags & POSTSTATE) && cp > min) {
        assert(cp >= realmin);
        cp--;  // Back up to actual match end
    } else if (cp == v->stop && max == v->stop) {
        // Handle end-of-string boundary
        co = d->cnfa->eos[(v->eflags & REG_NOTEOL) ? 0 : 1];
        ss = miss(v, d, css, co, cp, start);
        if ((ss == NULL || !(ss->flags & POSTSTATE)) && hitstopp != NULL)
            *hitstopp = 1;
    }

    // Final validation
    if (ss == NULL || !(ss->flags & POSTSTATE))
        return NULL;

    return cp;  // Return end position of shortest match
}
```