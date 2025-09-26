# byword

## Location
src/timezone/zic.c: 3651 - 3716

## Overview
A string lookup function that finds entries in lookup tables using case-insensitive exact or prefix matching, with special handling for timezone abbreviations and backward compatibility warnings.

## Definition
static const struct lookup *byword(const char *word, const struct lookup *table)

## Detailed Description
The byword function performs flexible string matching in lookup tables used by the timezone compiler (zic). It implements a two-phase search strategy: first attempting exact case-insensitive matches, then falling back to prefix matches if no exact match is found. The function includes special logic for handling "last" prefixes in day-of-week contexts and provides backward compatibility warnings for ambiguous abbreviations that existed in pre-2017c versions of zic.

The function handles three main scenarios:
1. Special "last" prefix processing for day-of-week tables
2. Exact case-insensitive matching using ciequal()
3. Prefix matching using ciprefix() with ambiguity detection

When multiple prefix matches are found, the function returns NULL to indicate ambiguity. For successful prefix matches, it optionally warns about potential compatibility issues with older zic versions.

## Parameters / Member Variables
- `word`: The input string to search for in the lookup table
- `table`: Pointer to an array of lookup structures to search through

## Dependencies
- Functions called/Symbols referenced:
  - ciprefix (case-insensitive prefix comparison)
  - ciequal (case-insensitive equality comparison)
  - itsabbr (checks if word is abbreviation of lookup entry)
  - warning (displays warning messages)
  - lookup (struct type for table entries)
- Called from (representative examples):
  - infile
  - getleapdatetime
  - inleap
  - rulesub

## Notes and Other Information
- Returns NULL for invalid input (NULL word or table)
- Special handling for "last" prefix transforms lookups from lasts table to wday_names table
- Warns about deprecated "last-" usage, recommending "last" prefix instead
- Provides backward compatibility warnings for abbreviations that were ambiguous in pre-2017c zic
- Part of the timezone compiler (zic) infrastructure for parsing timezone rule files
- Uses global variables `lasts`, `wday_names`, and `noise` for context-specific behavior