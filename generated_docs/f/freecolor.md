# freecolor

## Location
src/backend/regex/regc_color.c: 257 - 311

## Overview
Frees a color in a colormap structure, ensuring the color has no associated arcs or subcolors before deallocating it and managing the freelist.

## Definition


## Detailed Description
The freecolor function is responsible for properly deallocating a color from a colormap. Before freeing the color, it performs several safety checks to ensure the color is not actively being used (no arcs, no subcolors, no single/Unicode characters). The function manages a freelist data structure to efficiently track available color slots for reuse.

When freeing a color, the function handles two main scenarios:
1. If the color is at the maximum index (cm->max), it compacts the colormap by reducing the maximum and cleaning up the freelist
2. Otherwise, it simply adds the color to the freelist for later reuse

The function includes special handling for the WHITE color, which cannot be freed, and performs extensive freelist maintenance to keep the data structure consistent.

## Parameters / Member Variables
- : Pointer to the colormap structure containing the color to be freed
- : The color identifier to be freed (must be >= 0 and not WHITE)

## Dependencies
- Functions called/Symbols referenced:
  - UNUSEDCOLOR (macro to check if color is unused)
  - WHITE (constant representing the white color)
  - NOSUB (constant indicating no subcolor)
  - FREECOL (flag indicating a free color)
- Called from (representative examples):
  - [okcolors](../o/okcolors.md) (at src/backend/regex/regc_color.c:961)

## Notes and Other Information
- The function includes multiple assertions to ensure the color is in a valid state before freeing
- WHITE color is protected and cannot be freed
- The function maintains the integrity of the freelist by removing entries that exceed the current maximum
- This is part of PostgreSQL's regular expression engine color management system
- The function is static, meaning it's only used within the regc_color.c file