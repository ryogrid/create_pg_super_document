# cpstrdup

## Location
[src/backend/tsearch/spell.c:162-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L162-L174)

## Overview
cpstrdup is a static utility function that duplicates a C string using the ISpell dictionary's custom memory allocation system.

## Definition
```c
static char *cpstrdup(IspellDict *Conf, const char *str)
```

## Detailed Description
This function creates a duplicate copy of a null-terminated C string using the custom memory allocation function cpalloc. It allocates memory sufficient to hold the string plus the null terminator, then copies the source string to the newly allocated memory. The function is designed specifically for use within the ISpell dictionary system and uses the dictionary's memory management context.

## Parameters / Member Variables
- `Conf`: Pointer to an IspellDict structure (parameter present for consistency with other cp-prefixed allocation functions, though not directly used in this implementation)
- `str`: Pointer to the null-terminated source string to be duplicated

## Dependencies
- Functions called/Symbols referenced:
  - cpalloc (custom allocation function)
  - strlen (standard C library function)
  - strcpy (standard C library function)
  - IspellDict (struct type)
- Called from (representative examples):
  - [NIAddSpell](../N/NIAddSpell.md)
  - [NIAddAffix](../N/NIAddAffix.md)
  - [setCompoundAffixFlagValue](../s/setCompoundAffixFlagValue.md)
  - [NIImportOOAffixes](../N/NIImportOOAffixes.md)
  - [NISortDictionary](../N/NISortDictionary.md)

## Notes and Other Information
- This is a static function, only accessible within the spell.c compilation unit
- Uses the cp-prefixed allocation family which manages memory within the ISpell dictionary context
- Essential for string duplication operations during dictionary building and processing
- The function follows the standard strdup pattern but uses custom memory allocation
- Memory allocated by this function will be managed by the ISpell dictionary's memory context system