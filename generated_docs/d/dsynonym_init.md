# dsynonym_init

## Location
src/backend/tsearch/dict_synonym.c: 92 - 209

## Overview
Initializes a synonym dictionary by parsing a configuration file and building an internal data structure for efficient synonym lookups.

## Definition


## Detailed Description
This function is the initialization entry point for PostgreSQL's synonym dictionary. It processes configuration parameters, reads a synonym file, and constructs a sorted array of synonym mappings for efficient lookup during text search operations.

The function performs these key operations:
1. Parses dictionary options (synonyms file path, case sensitivity)
2. Opens and reads the specified synonym file line by line
3. Extracts word pairs from each line using findwrd()
4. Builds an array of Syn structures containing input/output word mappings
5. Sorts the array for efficient binary search during lexicalization
6. Handles case sensitivity options and prefix flags

The synonym file format expects each line to contain an input word followed by its replacement word, separated by whitespace.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - dictoptions: List of configuration parameters

## Dependencies
- Functions called/Symbols referenced:
  - DefElem, List, ListCell (PostgreSQL list structures)
  - defGetString, defGetBoolean (configuration parsing)
  - get_tsearch_config_filename (file path resolution)
  - tsearch_readline_begin, tsearch_readline, tsearch_readline_end (file reading)
  - findwrd (word parsing)
  - palloc0, repalloc, pstrdup, lowerstr (memory and string management)
  - qsort with compareSyn (array sorting)
- Called from (representative examples):
  - PostgreSQL dictionary initialization system (no direct callers in provided data)

## Notes and Other Information
- This is a PostgreSQL function callable from SQL for dictionary creation
- Supports two configuration parameters: 'synonyms' (required file path) and 'casesensitive' (optional boolean)
- Dynamically grows the synonym array as needed, starting with 64 entries and doubling when full
- Case insensitive mode converts all words to lowercase for consistent matching
- Ignores empty lines and lines with only one word in the synonym file
- The resulting dictionary structure is optimized for fast binary search lookups