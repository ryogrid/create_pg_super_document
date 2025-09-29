# subcolor

## Location
[src/backend/regex/regc_color.c:336-365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L336-L365)

## Overview
Allocates a new subcolor for a specific character if necessary, working only with characters that map into the low color map.

## Definition

```c
static color
subcolor(struct colormap *cm, chr c)
```
## Detailed Description
The subcolor function is responsible for creating subcolors when needed for character-specific operations in the regular expression engine. It works exclusively with characters that fall within the "simple" character range (up to MAX_SIMPLE_CHR) and use the low color map (locolormap).

The function first determines the current color of the character, then attempts to create a new subcolor using newsub(). If the character is already in an open subcolor that matches what would be created, it returns that existing color to avoid redundant work.

When a new subcolor is successfully created, the function updates the character counts:
- Decrements nschrs for the original color
- Increments nschrs for the new subcolor
- Sets firstchr for the new subcolor if it's the first character assigned to it
- Updates the locolormap to point the character to its new subcolor

## Parameters / Member Variables
- : Pointer to the colormap structure containing color mappings
- : The character for which to allocate a subcolor (must be <= MAX_SIMPLE_CHR)

## Dependencies
- Functions called/Symbols referenced:
  - [newsub](../n/newsub.md) (creates a new subcolor)
  - CISERR (macro to check for compilation errors)
  - MAX_SIMPLE_CHR (maximum value for simple characters)
  - CHR_MIN (minimum character value)
  - COLORLESS (constant representing no color/error state)
- Called from (representative examples):
  - [subcolorcvec](subcolorcvec.md) (at src/backend/regex/regc_color.c:555)
  - [subcoloronechr](subcoloronechr.md) (at src/backend/regex/regc_color.c:640)
  - CNOERR (at src/backend/regex/regcomp.c:458)

## Notes and Other Information
- Only works with characters in the simple character range (low color map)
- Returns COLORLESS if an error occurs during subcolor allocation
- Optimizes by returning existing color if character is already in appropriate subcolor
- Maintains accurate character counts (nschrs) for both source and destination colors
- Updates the firstchr field when assigning the first character to a new subcolor
- Part of PostgreSQL's regex engine color management system for handling character classes and ranges

## Simplified Source
```c
static color subcolor(struct colormap *cm, chr c)
{
    color co;    // current color of c
    color sco;   // new subcolor

    assert(c <= MAX_SIMPLE_CHR);

    // Get current color and create new subcolor
    co = cm->locolormap[c - CHR_MIN];
    sco = newsub(cm, co);
    if (CISERR())
        return COLORLESS;

    // If already in correct subcolor, return it
    if (co == sco)
        return co;

    // Update character counts and mappings
    cm->cd[co].nschrs--;
    if (cm->cd[sco].nschrs == 0)
        cm->cd[sco].firstchr = c;
    cm->cd[sco].nschrs++;
    cm->locolormap[c - CHR_MIN] = sco;

    return sco;
}
```