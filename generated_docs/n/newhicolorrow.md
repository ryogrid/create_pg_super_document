# newhicolorrow

## Location
[src/backend/regex/regc_color.c:420-468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L420-L468)

## Overview
Creates a new row in the hicolormap by cloning an existing row, used for managing color mappings in PostgreSQL's regex engine.

## Definition

```c
static int
newhicolorrow(struct colormap *cm,
			  int oldrow)
```
## Detailed Description
The  function is part of PostgreSQL's regex color management system. It creates a new row in the hicolormap array by cloning data from an existing row. The function handles dynamic memory management by expanding the hicolormap storage when needed, doubling the array size when capacity is reached. After copying the row data, it updates color reference counts to maintain proper bookkeeping for the color management system.

## Parameters / Member Variables
- : Pointer to the colormap structure containing the hicolormap array and related metadata
- : Index of the existing row to clone from

## Dependencies
- Functions called/Symbols referenced:
  - CERR (error reporting macro)
  - REALLOC (memory reallocation macro)
  - memcpy (standard library function for memory copying)
  - REG_ESPACE (error code constant)
- Called from (representative examples):
  - [subcoloronechr](../s/subcoloronechr.md) (at lines 681, 707, 716)
  - [subcoloronerange](../s/subcoloronerange.md) (at lines 801, 832, 841, 859)

## Notes and Other Information
- Returns the array index of the newly created row, or 0 on error
- The function may relocate the hicolormap array in memory during expansion
- Includes overflow protection by checking against INT_MAX before allocation
- Updates color reference counts (nuchrs) for all colors in the cloned row to maintain proper reference tracking
- Part of the regex engine's color compression optimization system

## Simplified Source

```c
static int
newhicolorrow(struct colormap *cm, int oldrow) {
    int newrow = cm->hiarrayrows;
    color *newrowptr;
    int i;

    // Expand array if needed (double size when full)
    if (newrow >= cm->maxarrayrows) {
        // Check for overflow before allocation
        if (cm->maxarrayrows >= INT_MAX / (cm->hiarraycols * 2)) {
            CERR(REG_ESPACE);
            return 0;
        }

        // Reallocate with double the capacity
        color *newarray = (color *) REALLOC(cm->hicolormap,
                                           cm->maxarrayrows * 2 * cm->hiarraycols * sizeof(color));
        if (newarray == NULL) {
            CERR(REG_ESPACE);
            return 0;
        }

        cm->hicolormap = newarray;
        cm->maxarrayrows *= 2;
    }
    cm->hiarrayrows++;

    // Copy data from old row to new row
    newrowptr = &cm->hicolormap[newrow * cm->hiarraycols];
    memcpy(newrowptr, &cm->hicolormap[oldrow * cm->hiarraycols],
           cm->hiarraycols * sizeof(color));

    // Update reference counts for all colors in new row
    for (i = 0; i < cm->hiarraycols; i++) {
        cm->cd[newrowptr[i]].nuchrs++;
    }

    return newrow;
}
```