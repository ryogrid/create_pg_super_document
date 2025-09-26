# lowerstr_ctx

## Location
[src/backend/tsearch/spell.c:175-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L175-L186)

## Overview
lowerstr_ctx applies string lowercasing using lowerstr() while ensuring the result is allocated in the ISpell dictionary's build context for proper memory management.

## Definition
```c
static char *lowerstr_ctx(IspellDict *Conf, const char *src)
```

## Detailed Description
This function provides a context-aware wrapper around the lowerstr() function. It temporarily switches to the ISpell dictionary's build memory context before calling lowerstr(), ensuring that the lowercased string result is allocated in the appropriate memory context. After the lowerstr() operation completes, it restores the previous memory context. This pattern ensures proper memory management during dictionary building operations where temporary string manipulations need to be allocated in the build context.

## Parameters / Member Variables
- `Conf`: Pointer to an IspellDict structure containing the build context to use for memory allocation
- `src`: Pointer to the source null-terminated string to be converted to lowercase

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (PostgreSQL memory management)
  - [lowerstr](lowerstr.md) (string lowercasing function)
  - IspellDict (struct type)
- Called from (representative examples):
  - [NIImportDictionary](../N/NIImportDictionary.md)
  - [NIImportOOAffixes](../N/NIImportOOAffixes.md)

## Notes and Other Information
- This is a static function, only accessible within the spell.c compilation unit
- Essential for ensuring proper memory context management during dictionary building
- The function follows PostgreSQL's memory context switching pattern for temporary allocations
- Used primarily during dictionary import operations where case-insensitive processing is required
- The result string is allocated in the build context and will be cleaned up when NIFinishBuild() is called