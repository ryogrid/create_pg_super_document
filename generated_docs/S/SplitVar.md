# SplitVar

## Location
[src/backend/tsearch/spell.c:2285-2291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2285-L2291)

## Overview
SplitVar is a linked list structure used in PostgreSQL's text search spell checking functionality to store and manage word stems during compound word decomposition and morphological analysis.

## Definition

```c
typedef struct SplitVar
{
	int			nstem;
	int			lenstem;
	char	  **stem;
	struct SplitVar *next;
} SplitVar;
```
## Detailed Description
SplitVar represents a collection of word stems that result from splitting compound words or applying morphological rules during spell checking operations. It's used in the ISpell dictionary implementation to handle complex word forms that can be decomposed into multiple constituent parts. The structure maintains a dynamic array of stem strings and can be linked together to form chains of related stem collections.

The structure is primarily used during the word normalization process where compound words are broken down into their component stems, and various morphological variants are generated and stored for dictionary lookup and matching.

## Parameters / Member Variables
- `nstem`: Current number of stems stored in the stem array
- `lenstem`: Allocated capacity of the stem array (can be larger than nstem for efficiency)
- `**stem`: Dynamic array of pointers to null-terminated stem strings
- `*next`: Pointer to the next SplitVar structure in a linked list chain
## Dependencies
- Functions called/Symbols referenced:
  - Self-reference in next pointer field
- Called from (representative examples):
  - [CheckCompoundAffixes](../C/CheckCompoundAffixes.md)
  - CopyVar
  - AddStem
  - SplitToVariants
  - NINormalizeWord

## Notes and Other Information
- The structure uses PostgreSQL's memory management functions (palloc, repalloc, pfree)
- The stem array grows dynamically when more stems need to be added via the AddStem function
- Initial allocation size for new SplitVar instances is typically 16 stem pointers
- Used extensively in compound word processing and morphological analysis within the text search subsystem
- Part of the ISpell dictionary implementation in src/backend/tsearch/spell.c:2285