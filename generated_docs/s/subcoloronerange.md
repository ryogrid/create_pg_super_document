# subcoloronerange

## Location
[src/backend/regex/regc_color.c:747-884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L747-L884)

## Overview
Handles subcolor allocation for a character range, managing complex overlaps with existing colormap ranges and creating necessary NFA arcs.

## Definition

```c
static void
subcoloronerange(struct vars *v,
				 chr from,
				 chr to,
				 struct state *lp,
				 struct state *rp,
				 color *lastsubcolor)
```
## Detailed Description
The  function processes character ranges that are above MAX_SIMPLE_CHR, handling the complex logic of merging and splitting colormap ranges. It manages overlaps between the new target range and existing ranges in the colormap, potentially creating multiple new ranges to accommodate partial overlaps. The function can split existing ranges into up to three parts and create new ranges from scratch when the target range doesn't correspond to any existing range. It maintains proper hicolormap row associations by cloning rows as needed and calls subcoloronerow to handle the actual color processing.

## Parameters / Member Variables
- `*v`: Pointer to the regex compilation variables structure
- `from`: Starting character of the range (must be > MAX_SIMPLE_CHR)
- `to`: Ending character of the range (must be > from)
- `*lp`: Pointer to the source state for NFA arcs
- `*rp`: Pointer to the destination state for NFA arcs
- `*lastsubcolor`: Pointer to the last subcolor created (for optimization to avoid duplicate arcs)
## Dependencies
- Functions called/Symbols referenced:
  - [newhicolorrow](../n/newhicolorrow.md) (creates new rows in hicolormap, called at lines 801, 832, 841, 859)
  - [subcoloronerow](subcoloronerow.md) (processes a row for subcolor allocation)
  - MALLOC/FREE (memory allocation/deallocation)
  - CERR (error reporting macro)
  - MAX_SIMPLE_CHR (constant for simple character threshold)
- Called from (representative examples):
  - [subcolorcvec](subcolorcvec.md) (at line 569)

## Notes and Other Information
- Does not return a value (void function)
- Requires that  and  (enforced by assertions)
- Can potentially create up to 2N+1 result ranges when processing N non-adjacent existing ranges
- Uses a sophisticated algorithm to handle all possible overlap scenarios between new and existing ranges
- Updates the  variable during processing to track the remaining portion of the target range
- Manages dynamic memory allocation with space estimation based on worst-case scenario
- Uses assertion to verify space estimation was adequate
- Part of the regex engine's color management system for handling complex Unicode character ranges efficiently

## Simplified Source
```c
static void subcoloronerange(struct vars *v, chr from, chr to,
                            struct state *lp, struct state *rp,
                            color *lastsubcolor)
{
    struct colormap *cm = v->cm;

    assert(from > MAX_SIMPLE_CHR);
    assert(from < to);

    // Allocate space for potentially expanded ranges
    colormaprange *newranges = MALLOC((cm->numcmranges * 2 + 1) * sizeof(colormaprange));
    if (newranges == NULL) {
        CERR(REG_ESPACE);
        return;
    }

    int numnewranges = 0;
    int newrow;

    // Copy ranges before target range
    // ... [pre-target range copying] ...

    // Process overlapping ranges with complex splitting logic
    while (/* overlapping ranges exist */) {
        if (/* range before target */) {
            // Create new range for non-overlapping portion
            // ... [new range creation] ...
        }

        if (/* range fully contained */) {
            // Process existing range in-place
            // ... [in-place processing] ...
        } else {
            // Split existing range
            // ... [range splitting logic] ...
        }

        // Update colors for the processed row
        subcoloronerow(v, newrow, lp, rp, lastsubcolor);
    }

    // Handle any remaining portion of target range
    // ... [remaining range handling] ...

    // Copy remaining ranges after target
    // ... [post-target range copying] ...

    // Update colormap with new range structure
    if (cm->cmranges != NULL)
        FREE(cm->cmranges);
    cm->cmranges = newranges;
    cm->numcmranges = numnewranges;
}
```