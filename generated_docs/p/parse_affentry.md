# parse_affentry

## Location
src/backend/tsearch/spell.c: 914 - 1031

## Overview
Parses a single entry from an Ispell format .affix file, extracting the mask, find pattern, and replacement pattern components used for morphological analysis.

## Definition
```c
static bool parse_affentry(char *str, char *mask, char *find, char *repl)
```

## Detailed Description
This function implements a finite state machine to parse entries from Ispell-format affix files. Each affix entry follows the format: `<mask> > [-<find>,]<replace>`. The function processes the input string character by character, transitioning between different parsing states:

- PAE_WAIT_MASK: Waiting for the mask portion
- PAE_INMASK: Reading the mask characters
- PAE_WAIT_FIND: Waiting for the find pattern after '>'
- PAE_INFIND: Reading the find pattern (after '-')
- PAE_WAIT_REPL: Waiting for the replacement pattern
- PAE_INREPL: Reading the replacement pattern

The parser handles multibyte characters properly and validates syntax, reporting errors for malformed entries. Comments starting with '#' are ignored.

## Parameters / Member Variables
- `str`: Input string containing the affix entry to parse
- `mask`: Output buffer to store the extracted mask pattern
- `find`: Output buffer to store the find pattern (what to remove)
- `repl`: Output buffer to store the replacement pattern (what to add)

## Dependencies
- Functions called/Symbols referenced:
  - t_iseq: Character comparison for text search
  - t_isspace: Space character testing
  - t_isalpha: Alphabetic character testing
  - COPYCHAR: Macro for copying multibyte characters
  - pg_mblen: Get multibyte character length
  - ereport/elog: Error reporting functions
- Called from (representative examples):
  - NIImportAffixes: Imports affixes from configuration files

## Notes and Other Information
- Returns true if parsing was successful and at least mask and (find or repl) are non-empty
- Uses a state machine approach for robust parsing of complex affix syntax
- Handles multibyte character encodings properly through pg_mblen
- Part of PostgreSQL's full-text search (tsearch) spell-checking functionality
- Supports Ispell dictionary format for morphological analysis