# pg_re_flags

## Location
src/backend/utils/adt/regexp.c: 45 - 49

## Overview
The pg_re_flags structure encapsulates all the options of interest for PostgreSQL's regular expression functions, providing a consolidated way to pass compilation flags and behavioral options.

## Definition


## Detailed Description
The pg_re_flags structure is used throughout PostgreSQL's regular expression system to standardize the passing of compilation and execution options. It serves as a parameter container that combines Spencer's regex compilation flags with PostgreSQL-specific behavioral flags. This structure ensures consistent handling of regex options across all regex-related functions in the backend.

## Parameters / Member Variables
- : Integer containing compile flags that are passed directly to Spencer's regex code, controlling pattern compilation behavior such as case sensitivity, extended syntax, etc.
- : Boolean flag indicating whether the regular expression operation should be performed globally (i.e., for each occurrence in the string rather than just the first match)

## Dependencies
- Functions called/Symbols referenced:
  - (This is a simple struct definition with no direct function calls)
- Called from (representative examples):
  - parse_re_flags
  - textregexreplace
  - textregexreplace_extended
  - regexp_count
  - regexp_instr
  - regexp_like
  - regexp_match
  - regexp_matches
  - setup_regexp_matches
  - regexp_split_to_table
  - regexp_split_to_array
  - regexp_substr

## Notes and Other Information
This structure is fundamental to PostgreSQL's regex implementation and is used extensively throughout src/backend/utils/adt/regexp.c. The separation of Spencer's regex flags (cflags) from PostgreSQL's own behavioral flags (glob) provides clean abstraction and allows for easy extension of regex functionality. The structure is typically populated by parse_re_flags() function based on user-provided flag strings.