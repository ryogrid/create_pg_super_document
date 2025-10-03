# NIImportDictionary

## Location
[src/backend/tsearch/spell.c:518-602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L518-L602)

## Overview
Imports dictionary data from a .dict file into the temporary array Spell, parsing word entries and their associated affix flags for text search operations.

## Definition

```c
void
NIImportDictionary(IspellDict *Conf, const char *filename)
```
## Detailed Description
NIImportDictionary reads and processes a dictionary file line by line, extracting words and their associated affix flags. Each line in the dictionary file contains a word optionally followed by a slash (/) and affix flags. The function performs several key operations:

1. Opens the dictionary file using tsearch_readline facilities
2. Parses each line to separate the base word from affix flags (delimited by '/')
3. Validates affix flags to ensure they are single-byte printable characters
4. Converts words to lowercase for consistent storage
5. Adds each word-flag combination to the spell dictionary using NIAddSpell

The function handles malformed entries gracefully by truncating invalid flag sequences and removing trailing whitespace from words.

## Parameters / Member Variables
- `*Conf`: Pointer to IspellDict structure representing the current dictionary configuration
- `*filename`: Path to the .dict file to import (caller must have applied get_tsearch_config_filename)
## Dependencies
- Functions called/Symbols referenced:
  - [tsearch_readline_begin](../t/tsearch_readline_begin.md)
  - [tsearch_readline](../t/tsearch_readline.md)
  - [tsearch_readline_end](../t/tsearch_readline_end.md)
  - [findchar](../f/findchar.md)
  - [pg_mblen](../p/pg_mblen.md)
  - [t_isprint](../t/t_isprint.md)
  - [t_isspace](../t/t_isspace.md)
  - [lowerstr_ctx](../l/lowerstr_ctx.md)
  - [NIAddSpell](NIAddSpell.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [dispell_init](../d/dispell_init.md)

## Notes and Other Information
- The function expects the caller to have already applied get_tsearch_config_filename to the filename parameter
- Only single-byte encoded flags are allowed for performance reasons
- The function uses PostgreSQL's memory management (pfree) for cleanup
- Invalid characters in affix flags cause the flag string to be truncated at that point
- Trailing whitespace is automatically removed from dictionary words
- Error handling includes reporting file access issues with appropriate error codes