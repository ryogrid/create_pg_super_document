# want_bloat

## Location
src/timezone/zic.c: 644 - 649

## Overview
Determines whether the timezone compiler should generate "bloated" timezone data files with extra information for better compatibility and debugging purposes.

## Definition


## Detailed Description
The want_bloat function is a simple predicate that checks the global bloat variable to determine if the timezone compiler should include additional data in the output files. When bloat is enabled (>= 0), the generated timezone files will contain extra information that can improve compatibility with older systems or provide more detailed debugging information, at the cost of larger file sizes. The function serves as a centralized check used throughout the timezone compilation process.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ZIC_BLOAT_DEFAULT (default bloat configuration constant)
- Called from (representative examples):
  - [writezone](writezone.md) (multiple calls in src/timezone/zic.c at lines 2191, 2344, 2431)
  - [years_of_observations](../y/years_of_observations.md) (in src/timezone/zic.c at lines 3085, 3256)
  - [addtype](../a/addtype.md) (in src/timezone/zic.c at line 3368)

## Notes and Other Information
- Returns true if bloat is enabled (bloat >= 0), false otherwise
- Used extensively throughout the timezone compilation process to control output verbosity
- Affects the size and compatibility of generated timezone data files
- The bloat variable is typically set via command-line options in the zic utility