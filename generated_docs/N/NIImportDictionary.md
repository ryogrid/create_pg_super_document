# NIImportDictionary

## Location
src/backend/tsearch/spell.c: 518 - 602

## Overview
Imports dictionary data from a .dict file into the temporary array Spell, parsing word entries and their associated affix flags for text search operations.

## Definition


## Detailed Description
NIImportDictionary reads and processes a dictionary file line by line, extracting words and their associated affix flags. Each line in the dictionary file contains a word optionally followed by a slash (/) and affix flags. The function performs several key operations:

1. Opens the dictionary file using tsearch_readline facilities
2. Parses each line to separate the base word from affix flags (delimited by '/')
3. Validates affix flags to ensure they are single-byte printable characters
4. Converts words to lowercase for consistent storage
5. Adds each word-flag combination to the spell dictionary using NIAddSpell

The function handles malformed entries gracefully by truncating invalid flag sequences and removing trailing whitespace from words.

## Parameters / Member Variables
- : Pointer to IspellDict structure representing the current dictionary configuration
- : Path to the .dict file to import (caller must have applied get_tsearch_config_filename)

## Dependencies
- Functions called/Symbols referenced:
  - tsearch_readline_begin
  - tsearch_readline
  - tsearch_readline_end
  - findchar
  - pg_mblen
  - t_isprint
  - t_isspace
  - lowerstr_ctx
  - NIAddSpell
  - pfree
- Called from (representative examples):
  - dispell_init

## Notes and Other Information
- The function expects the caller to have already applied get_tsearch_config_filename to the filename parameter
- Only single-byte encoded flags are allowed for performance reasons
- The function uses PostgreSQL's memory management (pfree) for cleanup
- Invalid characters in affix flags cause the flag string to be truncated at that point
- Trailing whitespace is automatically removed from dictionary words
- Error handling includes reporting file access issues with appropriate error codes