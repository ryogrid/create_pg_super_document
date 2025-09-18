# dumpcolors

## Location
src/backend/regex/regc_color.c: 1127 - 1190

## Overview
A debugging function that outputs a human-readable representation of a colormap structure, showing color assignments and character mappings for regex engine diagnostics.

## Definition
```c
static void dumpcolors(struct colormap *cm, FILE *f)
```

## Detailed Description
The dumpcolors function provides comprehensive debugging output for colormap structures used in PostgreSQL's regular expression engine. It displays detailed information about how characters are mapped to colors, which is essential for understanding and debugging regex compilation.

The function operates in two main phases:
1. **Basic color dump**: Iterates through all defined colors (skipping color 0), displaying each color's ID, character count, and the specific characters assigned to that color. Pseudo colors are specially marked with "(ps)" notation.
2. **High colormap dump**: If the colormap contains multi-byte character mappings (indicated by hiarrayrows > 1 or hiarraycols > 1), it dumps the high colormap table showing character ranges and their corresponding color assignments.

For each color, the function scans through all simple characters (CHR_MIN to MAX_SIMPLE_CHR) to identify which characters belong to that color, providing a complete view of the character-to-color mapping.

## Parameters / Member Variables
- `cm`: Pointer to the colormap structure to be dumped
- `f`: File pointer where the debugging output will be written (typically stderr or a debug file)

## Dependencies
- Functions called/Symbols referenced:
  - fprintf (for formatted output)
  - [CDEND](../C/CDEND.md) (macro to get end of color descriptor array)
  - UNUSEDCOLOR (macro to check if a color is unused)
  - GETCOLOR (macro to get color assignment for a character)
  - [dumpchr](dumpchr.md) (to output character representations)
  - CHR_MIN, MAX_SIMPLE_CHR (character range constants)
  - PSEUDO (flag indicating pseudo colors)
- Called from (representative examples):
  - [dumpnfa](dumpnfa.md) (src/backend/regex/regc_nfa.c:3684)
  - [dump](dump.md) (src/backend/regex/regcomp.c:2517)

## Notes and Other Information
- This is a debugging function used for development and troubleshooting of the regex engine
- The output format includes color numbers, character counts, and individual character listings
- Handles both simple character mappings and complex multi-byte character range mappings
- Pseudo colors are specially identified in the output as they represent abstract character classes
- The function may be computationally expensive for large character sets as it scans all simple characters for each color
- Color 0 is intentionally skipped as it represents a special default/background color