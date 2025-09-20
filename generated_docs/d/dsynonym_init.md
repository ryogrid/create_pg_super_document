# dsynonym_init

## Location
[src/backend/tsearch/dict_synonym.c:92-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_synonym.c#L92-L209)

## Overview
Initializes a synonym dictionary by parsing a configuration file and building an internal data structure for efficient synonym lookups.

## Definition

```c
Datum
dsynonym_init(PG_FUNCTION_ARGS)
```
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
  - [DefElem](../D/DefElem.md), List, ListCell (PostgreSQL list structures)
  - [defGetString](defGetString.md), defGetBoolean (configuration parsing)
  - [get_tsearch_config_filename](../g/get_tsearch_config_filename.md) (file path resolution)
  - [tsearch_readline_begin](../t/tsearch_readline_begin.md), tsearch_readline, tsearch_readline_end (file reading)
  - [findwrd](../f/findwrd.md) (word parsing)
  - [palloc0](../p/palloc0.md), repalloc, pstrdup, lowerstr (memory and string management)
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