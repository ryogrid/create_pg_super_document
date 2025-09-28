# longest

## Location
[src/backend/regex/rege_dfa.c:42-203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L42-L203)

## Overview
Implements the longest-preferred matching engine for DFA-based regular expression matching in PostgreSQL.

## Definition
```c
static chr *
longest(struct vars *v,
        struct dfa *d,
        chr *start,        /* where the match should start */
        chr *stop,         /* match must end at or before here */
        int *hitstopp)     /* record whether hit v->stop, if non-NULL */
```

## Detailed Description
The `longest` function is the core longest-preferred matching engine for DFA-based regular expression execution. It processes input text character by character, maintaining DFA state sets and tracking the longest possible match. The function handles special cases including backref matching and "matchall" NFAs (patterns that match any character sequence). It uses an optimized main loop with optional tracing support for debugging and returns the endpoint of the longest match found, or NULL if no match exists.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex execution context and state
- `d`: Pointer to DFA structure containing the compiled automaton 
- `start`: Starting character position where matching should begin
- `stop`: Ending character position where matching must end at or before
- `hitstopp`: Optional pointer to record whether matching hit the global stop position

## Dependencies
- Functions called/Symbols referenced:
  - [dfa_backref](../d/dfa_backref.md) (for handling backreferences)
  - [initialize](../i/initialize.md) (for setting up initial DFA state)
  - [miss](../m/miss.md) (for handling state transitions)
  - GETCOLOR (for character-to-color mapping)
  - FDEBUG (for debug tracing)
- Called from (representative examples):
  - LOFF (regex execution offset function)
  - [find](../f/find.md) (main regex search function)
  - [cfindloop](../c/cfindloop.md) (complex find loop)
  - [ccondissect](../c/ccondissect.md), caltdissect, citerdissect (dissection functions)

## Notes and Other Information
- Contains specialized fast paths for backref patterns and MATCHALL NFAs
- Includes conditional debug tracing code that can be enabled via REG_FTRACE
- The main scanning loop is duplicated to avoid trace overhead in production
- Returns match endpoint on success, NULL on failure or no match
- Essential component of PostgreSQLs regex execution engine for longest-match semantics

## Simplified Source

```c
// Simplified version of longest - longest-preferred matching engine
static chr *
longest(struct vars *v, struct dfa *d, chr *start, chr *stop, int *hitstopp) {
    chr *cp;
    chr *realstop = (stop == v->stop) ? stop : stop + 1;
    color co;
    struct sset *css;
    struct sset *ss;
    chr *post;
    int i;
    struct colormap *cm = d->cm;

    // Initialize hit indicator
    if (hitstopp != NULL)
        *hitstopp = 0;

    // Fast path: Handle backref to known string
    if (d->backno >= 0 && v->pmatch[d->backno].rm_so >= 0) {
        cp = dfa_backref(v, d, start, start, stop, false);
        if (cp == v->stop && stop == v->stop && hitstopp != NULL)
            *hitstopp = 1;
        return cp;
    }

    // Fast path: Handle matchall NFAs (patterns matching any characters)
    if (d->cnfa->flags & MATCHALL) {
        size_t nchr = stop - start;

        if (nchr < d->cnfa->minmatchall)
            return NULL;

        // Handle unlimited matches or bounded matches
        if (d->cnfa->maxmatchall == DUPINF) {
            if (stop == v->stop && hitstopp != NULL)
                *hitstopp = 1;
        } else {
            if (stop == v->stop && nchr <= d->cnfa->maxmatchall + 1 && hitstopp != NULL)
                *hitstopp = 1;
            if (nchr > d->cnfa->maxmatchall)
                return start + d->cnfa->maxmatchall;
        }
        return stop;
    }

    // Initialize DFA state
    css = initialize(v, d, start);
    if (css == NULL)
        return NULL;
    cp = start;

    // Handle startup transition
    if (cp == v->start) {
        co = d->cnfa->bos[(v->eflags & REG_NOTBOL) ? 0 : 1];
    } else {
        co = GETCOLOR(cm, *(cp - 1));
    }
    css = miss(v, d, css, co, cp, start);
    if (css == NULL)
        return NULL;
    css->lastseen = cp;

    // Main text-scanning loop
    while (cp < realstop) {
        co = GETCOLOR(cm, *cp);
        ss = css->outs[co];

        // Handle state transition miss
        if (ss == NULL) {
            ss = miss(v, d, css, co, cp + 1, start);
            if (ss == NULL)
                break;
        }

        cp++;
        ss->lastseen = cp;
        css = ss;
    }

    if (ISERR())
        return NULL;

    // Handle end-of-string shutdown
    if (cp == v->stop && stop == v->stop) {
        if (hitstopp != NULL)
            *hitstopp = 1;
        co = d->cnfa->eos[(v->eflags & REG_NOTEOL) ? 0 : 1];
        ss = miss(v, d, css, co, cp, start);
        if (ISERR())
            return NULL;

        // Check for match at end-of-line
        if (ss != NULL && (ss->flags & POSTSTATE))
            return cp;
        else if (ss != NULL)
            ss->lastseen = cp;
    }

    // Find the last (longest) match among all final states
    post = d->lastpost;
    for (ss = d->ssets, i = d->nssused; i > 0; ss++, i--) {
        if ((ss->flags & POSTSTATE) && post != ss->lastseen &&
            (post == NULL || post < ss->lastseen)) {
            post = ss->lastseen;
        }
    }

    if (post != NULL)
        return post - 1;  // Return endpoint of longest match

    return NULL;  // No match found
}
```

Key simplifications made:
- Removed debug tracing code and conditional compilation blocks
- Consolidated the duplicated main scanning loops into a single version
- Added descriptive comments for major logic sections
- Simplified variable declarations and removed some temporary variables
- Streamlined the matchall NFA handling logic
- Removed detailed debug output calls while preserving core algorithm
- Maintained all essential logic paths and error handling