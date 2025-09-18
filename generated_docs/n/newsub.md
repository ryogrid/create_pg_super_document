# newsub

## Location
[src/backend/regex/regc_color.c:389-419](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L389-L419)

## Overview
Allocates a new subcolor for a given color if necessary, implementing an optimization to avoid creating subcolors for singly-referenced colors.

## Definition


## Detailed Description
The newsub function is a core utility for creating subcolors in PostgreSQL's regex engine. It implements a key optimization strategy: if a color is referenced by only one character (nschrs + nuchrs == 1), it returns the original color instead of creating a subcolor, since there's no benefit to subdividing a color that's already atomic.

When a subcolor does need to be created, the function:
1. Checks if the color already has an open subcolor (sub != NOSUB)
2. If not, and the color has multiple references, creates a new subcolor
3. Links the original color to the new subcolor (co.sub = sco)
4. Makes the subcolor self-referential (sco.sub = sco) to indicate it's an "open" subcolor

This design allows multiple characters or character ranges to be moved into the same subcolor as needed, while maintaining the parent-child relationship in the color hierarchy.

## Parameters / Member Variables
- : Pointer to the colormap structure containing color information
- : The color for which to allocate a subcolor

## Dependencies
- Functions called/Symbols referenced:
  - [newcolor](newcolor.md) (allocates a new color)
  - NOSUB (constant indicating no subcolor exists)
  - COLORLESS (constant representing no color/error state)
  - CISERR (macro to check for compilation errors)
- Called from (representative examples):
  - [subcolor](../s/subcolor.md) (at src/backend/regex/regc_color.c:344)
  - [subcolorhi](../s/subcolorhi.md) (at src/backend/regex/regc_color.c:372)
  - [EventTriggerCollectAlterTableSubcmd](../E/EventTriggerCollectAlterTableSubcmd.md) (at src/backend/commands/event_trigger.c:1681+)
  - [maybe_reread_subscription](../m/maybe_reread_subscription.md) (at src/backend/replication/logical/worker.c:3878+)

## Notes and Other Information
- Implements important optimization: avoids creating subcolors for singly-referenced colors
- Open subcolors are self-referential (sub field points to themselves)
- Returns the original color if no subcolor is needed
- Returns COLORLESS if an error occurs during color allocation
- Part of PostgreSQL's regex engine color management system
- The function name appears in other contexts (event triggers, replication) but this analysis focuses on the regex engine implementation
- Essential for efficiently managing color hierarchies in complex regular expressions