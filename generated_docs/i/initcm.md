# initcm

## Location
[src/backend/regex/regc_color.c:49-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L49-L102)

## Overview
Initializes a new colormap structure for regular expression compilation, setting up memory allocations and default color mappings.

## Definition

```c
static void
initcm(struct vars *v,
	   struct colormap *cm)
```
## Detailed Description
The  function initializes a colormap structure which is used during regular expression compilation to map characters to colors for efficient pattern matching. It sets up two types of color mappings:

1. **Low-range mapping** (): Maps characters from CHR_MIN to MAX_SIMPLE_CHR directly to colors
2. **High-range mapping** (): Uses a 2D array structure for characters above MAX_SIMPLE_CHR

The function allocates memory for these mappings and initializes the WHITE color descriptor, which represents the default color for most characters. It also sets up various counters and flags used by the colormap management system.

## Parameters / Member Variables
- `*v`: Pointer to vars structure containing compilation context and error handling
- `*cm`: Pointer to colormap structure to be initialized
## Dependencies
- Functions called/Symbols referenced:
  - MALLOC (memory allocation)
  - CERR (error reporting)
  - memset (memory initialization)
- Called from (representative examples):
  - CNOERR (in regcomp.c)

## Notes and Other Information
- The function relies on WHITE being zero for efficient memory initialization using memset
- Memory allocation failures are handled gracefully by setting error codes and preventing crashes during cleanup
- The initial allocation uses an arbitrary size of 4 rows for the high-range color array
- All characters initially map to the WHITE color until specific color assignments are made during regex compilation

## Simplified Source

```c
static void
initcm(struct vars *v, struct colormap *cm)
{
    // Initialize basic colormap structure
    cm->magic = CMMAGIC;
    cm->v = v;
    cm->ncds = NINLINECDS;
    cm->cd = cm->cdspace;
    cm->max = 0;
    cm->free = 0;

    // Set up WHITE color descriptor for default mapping
    struct colordesc *cd = cm->cd;
    cd->nschrs = MAX_SIMPLE_CHR - CHR_MIN + 1;
    cd->nuchrs = 1;
    cd->sub = NOSUB;
    cd->arcs = NULL;
    cd->firstchr = CHR_MIN;
    cd->flags = 0;

    // Allocate and initialize low-range character mapping
    cm->locolormap = (color *) MALLOC((MAX_SIMPLE_CHR - CHR_MIN + 1) * sizeof(color));
    if (cm->locolormap == NULL)
    {
        CERR(REG_ESPACE);
        cm->cmranges = NULL;
        cm->hicolormap = NULL;
        return;
    }
    memset(cm->locolormap, WHITE, (MAX_SIMPLE_CHR - CHR_MIN + 1) * sizeof(color));

    // Initialize class bits and range tracking
    memset(cm->classbits, 0, sizeof(cm->classbits));
    cm->numcmranges = 0;
    cm->cmranges = NULL;

    // Set up high-range character mapping
    cm->maxarrayrows = 4;  // Initial allocation size
    cm->hiarrayrows = 1;
    cm->hiarraycols = 1;
    cm->hicolormap = (color *) MALLOC(cm->maxarrayrows * sizeof(color));
    if (cm->hicolormap == NULL)
    {
        CERR(REG_ESPACE);
        return;
    }
    cm->hicolormap[0] = WHITE;  // Default for "all other characters"
}
```