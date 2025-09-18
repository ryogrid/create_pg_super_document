# NIAddSpell

## Location
src/backend/tsearch/spell.c: 487 - 517

## Overview
A static function that adds a new word entry with associated affix flags to the temporary spell dictionary array during dictionary loading.

## Definition
```c
static void NIAddSpell(IspellDict *Conf, const char *word, const char *flag)
```

## Detailed Description
The `NIAddSpell` function is responsible for dynamically adding word entries to the dictionary's temporary spell array during the dictionary import process. It manages memory allocation for the growing array and creates SPELL structures for each word.

The function implements dynamic array resizing: when the array reaches capacity (`nspell >= mspell`), it either doubles the size if the array already exists or initializes it with a size of 20,480 entries (1024 * 20). It uses different allocation strategies - `repalloc` for resizing existing arrays and `tmpalloc` for initial allocation and individual SPELL entries.

Each SPELL entry is allocated with enough space for the header (`SPELLHDRSZ`) plus the word string plus null terminator. The function copies the word and handles the flag parameter by either duplicating it using `cpstrdup` or setting it to `VoidString` if the flag is empty.

## Parameters / Member Variables
- `Conf`: Pointer to IspellDict structure containing the dictionary being built
- `word`: The word string to add to the dictionary
- `flag`: String containing affix flags associated with the word (can be empty)

## Dependencies
- Functions called/Symbols referenced:
  - IspellDict (structure type)
  - SPELL (structure type for word entries)
  - repalloc (PostgreSQL memory reallocation function)
  - tmpalloc (PostgreSQL temporary memory allocation function)
  - SPELLHDRSZ (constant for SPELL structure header size)
  - strcpy (standard C library function)
  - strlen (standard C library function)
  - cpstrdup (custom string duplication function)
  - VoidString (constant for empty string representation)
- Called from (representative examples):
  - NIImportDictionary

## Notes and Other Information
- Dynamically grows the Spell array as needed using a doubling strategy
- Initial array size is set to 20,480 entries when first allocated
- Uses temporary memory allocation for both the array and individual entries
- Handles empty flags by setting them to VoidString rather than duplicating empty strings
- Located in src/backend/tsearch/spell.c:487-517