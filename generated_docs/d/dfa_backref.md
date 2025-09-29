# dfa_backref

## Location
[src/backend/regex/rege_dfa.c:506-584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L506-L584)

## Overview
Finds the best match length for a known backreference string without running the full NFA, providing optimized backref matching.

## Definition
```c
static chr *
dfa_backref(struct vars *v,
            struct dfa *d,
            chr *start,        /* where the match should start */
            chr *min,          /* match must end at or after here */
            chr *max,          /* match must end at or before here */
            bool shortest)
```

## Detailed Description
The `dfa_backref` function provides an optimized path for matching backreferences when the referenced string is already known. Instead of executing the backref nodes full NFA, it directly compares the target string against repeated instances of the backreferenced substring. This approach is significantly more efficient than general NFA execution. The function handles repetition bounds (min/max), supports both shortest and longest matching preferences, and includes special handling for zero-length backreferences.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex execution context and match results
- `d`: Pointer to DFA structure containing backref metadata (backno, backmin, backmax)
- `start`: Starting character position where backreference matching should begin
- `min`: Minimum ending position - match must end at or after this point
- `max`: Maximum ending position - match must end at or before this point
- `shortest`: Boolean flag indicating whether to prefer shortest or longest match

## Dependencies
- Functions called/Symbols referenced:
  - [compare](../c/compare.md) (string comparison function from regex globals)
  - [shortest](../s/shortest.md) (called recursively for some backref scenarios)
  - DUPINF (infinity constant for unlimited repetitions)
- Called from (representative examples):
  - [longest](../l/longest.md) (longest-match engine for backref optimization)
  - [shortest](../s/shortest.md) (shortest-match engine for backref optimization)
  - LOFF (regex execution offset function)

## Notes and Other Information
- Synchronized with cbrdissect() which performs similar validation for predetermined string sections
- Handles zero-length backreferences as special case (matches empty string with any repetition count)
- Calculates valid repetition ranges based on string length and position constraints
- Performs actual string content comparison only after validating repetition bounds
- Essential optimization for backref performance in PostgreSQL regex engine
- Returns match endpoint for valid matches, NULL for invalid or impossible matches

## Simplified Source
```c
static chr *dfa_backref(struct vars *v, struct dfa *d,
                       chr *start, chr *min, chr *max, bool shortest)
{
    int n = d->backno;
    int backmin = d->backmin;
    int backmax = d->backmax;
    size_t numreps, minreps, maxreps;
    size_t brlen;
    chr *brstring;
    chr *p;

    // Get the backreferenced string
    if (v->pmatch[n].rm_so == -1)
        return NULL;
    brstring = v->start + v->pmatch[n].rm_so;
    brlen = v->pmatch[n].rm_eo - v->pmatch[n].rm_so;

    // Special case: zero-length backreference
    if (brlen == 0) {
        if (min == start && backmin <= backmax)
            return start;
        return NULL;
    }

    // Calculate min/max repetitions based on position constraints
    if (min <= start)
        minreps = 0;
    else
        minreps = (min - start - 1) / brlen + 1;
    maxreps = (max - start) / brlen;

    // Apply backref bounds
    if (minreps < backmin)
        minreps = backmin;
    if (backmax != DUPINF && maxreps > backmax)
        maxreps = backmax;
    if (maxreps < minreps)
        return NULL;

    // Quick exit for zero-repetitions in shortest mode
    if (shortest && minreps == 0)
        return start;

    // Compare actual string contents
    p = start;
    numreps = 0;
    while (numreps < maxreps) {
        if ((*v->g->compare)(brstring, p, brlen) != 0)
            break;
        p += brlen;
        numreps++;
        if (shortest && numreps >= minreps)
            break;
    }

    if (numreps >= minreps)
        return p;
    return NULL;
}
```

This function optimizes backreference matching by:
1. Extracting the previously matched backreference string
2. Calculating valid repetition ranges based on position and length constraints
3. Directly comparing string content instead of running full NFA
4. Supporting both shortest and longest match preferences
5. Handling edge cases like zero-length backreferences