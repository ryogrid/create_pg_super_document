# subcoloronerange

## Location
[src/backend/regex/regc_color.c:747-884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L747-L884)

## Overview
Handles subcolor allocation for a character range, managing complex overlaps with existing colormap ranges and creating necessary NFA arcs.

## Definition


## Detailed Description
The  function processes character ranges that are above MAX_SIMPLE_CHR, handling the complex logic of merging and splitting colormap ranges. It manages overlaps between the new target range and existing ranges in the colormap, potentially creating multiple new ranges to accommodate partial overlaps. The function can split existing ranges into up to three parts and create new ranges from scratch when the target range doesn't correspond to any existing range. It maintains proper hicolormap row associations by cloning rows as needed and calls subcoloronerow to handle the actual color processing.

## Parameters / Member Variables
- : Pointer to the regex compilation variables structure
- : Starting character of the range (must be > MAX_SIMPLE_CHR)
- : Ending character of the range (must be > from)
- : Pointer to the source state for NFA arcs
- : Pointer to the destination state for NFA arcs
- : Pointer to the last subcolor created (for optimization to avoid duplicate arcs)

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