# AddStem

## Location
[src/backend/tsearch/spell.c:2361-2373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2361-L2373)

## Overview
Adds a word stem to a SplitVar structure, automatically expanding the storage capacity when needed during PostgreSQL's text search spell checking operations.

## Definition
```c
static void AddStem(SplitVar *v, char *word)
```

## Detailed Description
AddStem appends a new word stem to the dynamic array maintained within a SplitVar structure. The function implements automatic capacity expansion using a doubling strategy when the current array is full. This ensures efficient memory usage while avoiding frequent reallocations. The function is a core utility for building collections of word stems during spell checking and dictionary processing operations.

## Parameters / Member Variables
- `v`: Target SplitVar structure to add the stem to
- `word`: Word stem string to be added to the collection

## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](../r/repalloc.md) (memory reallocation for capacity expansion)
  - [SplitVar](../S/SplitVar.md) (structure type)
- Called from (representative examples):
  - [SplitToVariants](../S/SplitToVariants.md) (at src/backend/tsearch/spell.c:2439, 2490, 2504, 2518)

## Notes and Other Information
- Uses a doubling strategy for capacity expansion (lenstem *= 2) to achieve amortized O(1) insertion time
- Does not duplicate the word string - stores the pointer directly
- Caller is responsible for ensuring the word pointer remains valid
- Part of PostgreSQL's text search infrastructure for managing word variants during spell checking