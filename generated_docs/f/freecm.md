# freecm

## Location
[src/backend/regex/regc_color.c:103-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L103-L119)

## Overview
Frees all dynamically allocated memory associated with a colormap structure during regular expression cleanup.

## Definition
```c
static void freecm(struct colormap *cm)
```

## Detailed Description
The `freecm` function is responsible for releasing all dynamically allocated memory within a colormap structure. It performs selective cleanup by checking each memory allocation before freeing it:

1. **Magic number invalidation**: Sets the magic number to 0 to mark the structure as invalid
2. **Color descriptor array**: Frees the dynamically allocated color descriptor array if it was expanded beyond the inline storage
3. **Low-range color map**: Frees the array used for mapping simple characters to colors
4. **Character ranges**: Frees the array used for storing character range information
5. **High-range color map**: Frees the 2D array used for mapping complex characters to colors

The function is designed to be safe to call even if some allocations failed during initialization, as it checks each pointer before freeing.

## Parameters / Member Variables
- `cm`: Pointer to colormap structure to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - FREE (memory deallocation macro)
- Called from (representative examples):
  - [rfree](../r/rfree.md) (in regcomp.c)

## Notes and Other Information
- The function safely handles partial initialization scenarios where some allocations may have failed
- Magic number invalidation helps detect use-after-free bugs in debug builds
- The function only frees dynamically allocated arrays, not the inline storage (cdspace)
- Memory cleanup is essential to prevent memory leaks in regular expression compilation

## Simplified Source

```c
static void
freecm(struct colormap *cm)
{
    // Invalidate magic number to mark structure as freed
    cm->magic = 0;

    // Free dynamically allocated color descriptor array if expanded
    if (cm->cd != cm->cdspace)
        FREE(cm->cd);

    // Free character mapping arrays
    if (cm->locolormap != NULL)
        FREE(cm->locolormap);

    if (cm->cmranges != NULL)
        FREE(cm->cmranges);

    if (cm->hicolormap != NULL)
        FREE(cm->hicolormap);
}
```