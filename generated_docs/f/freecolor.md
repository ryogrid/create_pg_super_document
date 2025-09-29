# freecolor

## Location
[src/backend/regex/regc_color.c:257-311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L257-L311)

## Overview
Frees a color in a colormap structure, ensuring the color has no associated arcs or subcolors before deallocating it and managing the freelist.

## Definition

```c
static void
freecolor(struct colormap *cm,
		  color co)
```
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

## Simplified Source

```c
static void freecolor(struct colormap *cm, color co) {
    struct colordesc *cd = &cm->cd[co];

    // Cannot free the WHITE color
    if (co == WHITE)
        return;

    // Mark color as free
    cd->flags = FREECOL;

    if ((size_t) co == cm->max) {
        // Color is at max index - compact the colormap
        while (cm->max > WHITE && UNUSEDCOLOR(&cm->cd[cm->max]))
            cm->max--;

        // Clean up freelist entries beyond new max
        while ((size_t) cm->free > cm->max)
            cm->free = cm->cd[cm->free].sub;

        // Remove freelist entries that exceed max
        if (cm->free > 0) {
            color prev_color = cm->free;
            color next_color = cm->cd[prev_color].sub;
            while (next_color > 0) {
                if ((size_t) next_color > cm->max) {
                    // Remove this entry from freelist
                    next_color = cm->cd[next_color].sub;
                    cm->cd[prev_color].sub = next_color;
                } else {
                    prev_color = next_color;
                    next_color = cm->cd[prev_color].sub;
                }
            }
        }
    } else {
        // Add color to freelist
        cd->sub = cm->free;
        cm->free = (color) (cd - cm->cd);
    }
}
```