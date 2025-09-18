# pg_reg_getcharacters

## Location
src/backend/regex/regexport.c: 266 - 293

## Overview
Retrieves all character codes that belong to a specific color in a regular expressions colormap, writing them into a provided array.

## Definition
```c
void pg_reg_getcharacters(const regex_t *regex, int co, pg_wchar *chars, int chars_len)
```

## Detailed Description
This function extracts all character codes that are assigned to a specific color number in the regular expressions internal colormap. The colormap is a data structure used by PostgreSQLs regex engine to group characters with similar matching properties for optimization purposes.

The function performs a linear scan through the low character map (covering characters from CHR_MIN to MAX_SIMPLE_CHR) and copies matching character codes into the provided buffer. This is described as a "relatively expensive operation" due to the linear scan required.

The function includes several safety checks:
- Validates the regex structure using the REMAGIC check
- Ensures the color number is within valid range (1 to cm->max)
- Refuses to process pseudocolors (those with the PSEUDO flag set)
- Respects the buffer length limit to prevent overflow

Note that this function only examines the low character map, as there should not be any matching entries in the high character map for the same color.

## Parameters / Member Variables
- `regex`: Pointer to a compiled regular expression structure containing the colormap
- `co`: Color number to retrieve characters for (must be > 0 and <= cm->max)
- `chars`: Output buffer to store the character codes (pg_wchar array)
- `chars_len`: Maximum number of characters that can be stored in the chars buffer

## Dependencies
- Functions called/Symbols referenced:
  - regex_t (regular expression structure type)
  - colormap (color mapping structure)
  - chr (character type)
  - REMAGIC (magic number constant for regex validation)
  - guts (internal regex data structure)
  - PSEUDO (flag indicating pseudocolors)
  - MAX_SIMPLE_CHR (maximum simple character code)
  - CHR_MIN (minimum character code)

- Called from (representative examples):
  - regex_arc_t (referenced in regexport.h)

## Notes and Other Information
- This function is part of PostgreSQLs regex export API, designed to provide introspection into compiled regular expressions
- The function will not return characters for WHITE, RAINBOW, or pseudocolors as noted in the comments
- The chars_len parameter must be at least as large as indicated by pg_reg_getnumcharacters() to ensure all characters are returned
- Only characters in the "simple" range (CHR_MIN to MAX_SIMPLE_CHR) are examined; high characters are not processed
- The function stops filling the buffer when chars_len characters have been written, potentially truncating results if the buffer is too small
- Located in src/backend/regex/regexport.c:266-293