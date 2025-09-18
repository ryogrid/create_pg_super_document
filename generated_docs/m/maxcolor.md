# maxcolor

## Location
src/backend/regex/regc_color.c: 172 - 184

## Overview
Reports the highest color number currently in use within a colormap structure.

## Definition
```c
static color maxcolor(struct colormap *cm)
```

## Detailed Description
The `maxcolor` function provides a simple query interface to determine the maximum color value that has been assigned within a colormap. This information is essential for algorithms that need to iterate over all active colors or allocate data structures sized according to the color range.

The function performs error checking using the CISERR() macro before returning the value. If an error condition exists, it returns COLORLESS instead of the actual maximum color value, ensuring that calling code can detect error conditions.

## Parameters / Member Variables
- `cm`: Pointer to colormap structure to query

## Dependencies
- Functions called/Symbols referenced:
  - CISERR (error checking macro)
  - COLORLESS (constant representing invalid/no color)
- Called from (representative examples):
  - compact (in regc_nfa.c)

## Notes and Other Information
- Returns COLORLESS if the colormap is in an error state
- The `max` field in the colormap structure tracks the highest assigned color number
- Essential for NFA (Non-deterministic Finite Automaton) optimization algorithms that need to know the color space size
- Simple getter function that provides safe access to internal colormap state
- Used during regex compilation optimization phases