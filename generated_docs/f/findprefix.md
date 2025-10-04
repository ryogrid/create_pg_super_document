# findprefix

## Location
[src/backend/regex/regprefix.c:116-268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regprefix.c#L116-L268)

## Overview
A static helper function that performs the core analysis to extract a common prefix from a compiled NFA (Non-deterministic Finite Automaton) representation of a regular expression.

## Definition

```c
static int						/* regprefix return code */
findprefix(struct cnfa *cnfa,
		   struct colormap *cm,
		   chr *string,
		   size_t *slength)
```
## Detailed Description
The findprefix function implements the core algorithm for identifying common prefixes in regular expression patterns. It traverses the NFA state machine starting from the "pre" state, following transitions to identify sequences of characters that must appear at the beginning of any string matching the pattern.

The function first validates that the pattern is left-anchored by checking that the "pre" state only has BOS (Beginning of String) or BOL (Beginning of Line) outgoing arcs that lead to the same next state. It then follows the state transitions, collecting characters that form a mandatory prefix. The traversal continues until it encounters a state with multiple possible transitions, indicating the end of the common prefix.

The algorithm handles various edge cases including patterns with multiple parallel paths that converge on the same character, EOS/EOL terminations, and color-based character groupings in the NFA representation.

## Parameters / Member Variables
- `*cnfa`: Pointer to the compiled NFA structure representing the regular expression
- `*cm`: Pointer to the colormap structure that groups characters into equivalence classes
- `*string`: Pre-allocated character array where the prefix will be stored
- `*slength`: Pointer to size_t that tracks the current length of the prefix (must be preset to zero)
## Dependencies
- Functions called/Symbols referenced:
  - GETCOLOR (macro for character color lookup)
  - Various constants: COLORLESS, RAINBOW, REG_NOMATCH, REG_PREFIX, REG_EXACT
- Called from (representative examples):
  - [pg_regprefix](../p/pg_regprefix.md)

## Return Values
- : A common prefix was found and stored in the string array
- : The pattern requires an exact match (all strings matching the regex are identical)
- : No common prefix exists or pattern is not left-anchored

## Notes and Other Information
The function implements several sophisticated optimizations and handles corner cases in regex analysis. It uses a color-based character classification system where characters are grouped into equivalence classes for efficient processing. The algorithm can detect exact matches by checking if the final state only has EOS/EOL transitions leading to the "post" state. The function is designed to be conservative - it may miss some optimization opportunities but will never provide incorrect prefix information that could lead to false matches.

## Simplified Source

```c
static int findprefix(struct cnfa *cnfa, struct colormap *cm,
                      chr *string, size_t *slength)
{
    int st, nextst;
    color thiscolor;
    chr c;
    struct carc *ca;

    // Check that pattern is left-anchored (pre state has only BOS/BOL arcs)
    st = cnfa->pre;
    nextst = -1;
    for (ca = cnfa->states[st]; ca->co != COLORLESS; ca++)
    {
        if (ca->co == cnfa->bos[0] || ca->co == cnfa->bos[1])
        {
            if (nextst == -1)
                nextst = ca->to;
            else if (nextst != ca->to)
                return REG_NOMATCH;
        }
        else
            return REG_NOMATCH;
    }
    if (nextst == -1)
        return REG_NOMATCH;

    // Traverse states collecting common prefix characters
    do
    {
        st = nextst;
        nextst = -1;
        thiscolor = COLORLESS;

        // Examine all outgoing arcs from current state
        for (ca = cnfa->states[st]; ca->co != COLORLESS; ca++)
        {
            // Skip BOS/BOL arcs
            if (ca->co == cnfa->bos[0] || ca->co == cnfa->bos[1])
                continue;

            // Stop at EOS/EOL, RAINBOW, or LACON arcs
            if (ca->co == cnfa->eos[0] || ca->co == cnfa->eos[1] ||
                ca->co == RAINBOW || ca->co >= cnfa->ncolors)
            {
                thiscolor = COLORLESS;
                break;
            }

            // Track transition colors
            if (thiscolor == COLORLESS)
            {
                thiscolor = ca->co;
                nextst = ca->to;
            }
            else if (thiscolor == ca->co)
            {
                nextst = -1; // Multiple arcs with same color
            }
            else
            {
                thiscolor = COLORLESS; // Multiple different colors
                break;
            }
        }

        // Stop if no unique color or color isn't singleton
        if (thiscolor == COLORLESS)
            break;
        if (cm->cd[thiscolor].nschrs != 1 || cm->cd[thiscolor].nuchrs != 0)
            break;

        // Add the singleton character to prefix
        c = cm->cd[thiscolor].firstchr;
        if (GETCOLOR(cm, c) != thiscolor)
            break;
        string[(*slength)++] = c;

    } while (nextst != -1);

    // Check if we have an exact match (only EOS/EOL to post state)
    nextst = -1;
    for (ca = cnfa->states[st]; ca->co != COLORLESS; ca++)
    {
        if (ca->co == cnfa->eos[0] || ca->co == cnfa->eos[1])
        {
            if (nextst == -1)
                nextst = ca->to;
            else if (nextst != ca->to)
            {
                nextst = -1;
                break;
            }
        }
        else
        {
            nextst = -1;
            break;
        }
    }
    if (nextst == cnfa->post)
        return REG_EXACT;

    // Return result based on prefix length
    return (*slength > 0) ? REG_PREFIX : REG_NOMATCH;
}
```