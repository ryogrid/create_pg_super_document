# findarc

## Location
src/backend/regex/regc_nfa.c: 592 - 607

## Overview
Searches for and returns an arc from a given state that matches specified type and color criteria.

## Definition
```c
static struct arc *findarc(struct state *s, int type, color co)
```

## Detailed Description
The `findarc` function searches through the outgoing arcs of a specified state to find an arc that matches both a given type and color. In regular expression NFAs, arcs have types (such as PLAIN, EMPTY, etc.) and colors (which represent character classes or specific characters). This function provides a way to locate a specific arc based on these two key properties.

The function performs a linear search through the state's outgoing arc chain (`outs`). When it finds the first arc that matches both the specified type and color, it returns a pointer to that arc. If no matching arc is found, it returns NULL. The function documentation notes that if multiple arcs match the criteria, the result is random (meaning it returns the first one found, which depends on the order in the chain).

## Parameters / Member Variables
- `s`: The state whose outgoing arcs should be searched
- `type`: The arc type to match against
- `co`: The color to match against

## Dependencies
- Functions called/Symbols referenced:
  - struct arc (data structure)
  - struct state (data structure)
  - color (type definition)
- Called from (representative examples):
  - colorcomplement (in regc_color.c:1085)

## Notes and Other Information
- This is a static function internal to the regex NFA construction module
- Returns a pointer to the first matching arc, or NULL if no match is found
- If multiple arcs match the criteria, the function returns an arbitrary one (the first found)
- Used primarily during NFA construction and optimization phases
- Part of PostgreSQL's internal regular expression engine implementation
- The search is performed in the order arcs appear in the outgoing arc chain