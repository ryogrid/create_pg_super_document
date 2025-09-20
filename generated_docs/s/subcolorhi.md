# subcolorhi

## Location
[src/backend/regex/regc_color.c:366-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L366-L388)

## Overview
Allocates a new subcolor for a high colormap entry if necessary, performing the same processing as subcolor() but for entries that may represent multiple character codes.

## Definition

```c
static color
subcolorhi(struct colormap *cm, color *pco)
```
## Detailed Description
The subcolorhi function is the high colormap counterpart to the subcolor() function. While subcolor() works with individual characters in the low colormap, subcolorhi() operates on entries in the high colormap that may correspond to multiple character codes or Unicode ranges rather than single characters.

The function follows a similar pattern to subcolor():
1. Gets the current color from the provided pointer
2. Attempts to create a new subcolor using newsub()
3. If the color is already in an appropriate open subcolor, returns it immediately
4. Otherwise, updates the character counts and assigns the new subcolor

The key difference is that it works with nuchrs (Unicode/high character counts) instead of nschrs (single-byte character counts), and it updates the colormap entry through the provided pointer rather than directly indexing into locolormap.

## Parameters / Member Variables
- : Pointer to the colormap structure containing color mappings
- : Pointer to the color entry in the high colormap to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [newsub](../n/newsub.md) (creates a new subcolor)
  - CISERR (macro to check for compilation errors)
  - COLORLESS (constant representing no color/error state)
- Called from (representative examples):
  - [subcolorcvec](subcolorcvec.md) (at src/backend/regex/regc_color.c:599)
  - [subcoloronerow](subcoloronerow.md) (at src/backend/regex/regc_color.c:899)

## Notes and Other Information
- Designed for high colormap entries that may represent multiple character codes
- Works with nuchrs (Unicode character counts) instead of nschrs (single-byte character counts)
- Updates the colormap entry indirectly through the provided pointer
- Returns COLORLESS if an error occurs during subcolor allocation
- Optimizes by returning existing color if entry is already in appropriate subcolor
- Part of PostgreSQL's regex engine color management system for handling Unicode and multi-byte character mappings
- Does not set firstchr like subcolor() does, since high colormap entries may represent ranges rather than single characters