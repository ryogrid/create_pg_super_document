# CopyVar

## Location
src/backend/tsearch/spell.c: 2336 - 2360

## Overview
Creates a copy of a SplitVar structure, which is used in PostgreSQL's text search spell checking functionality to manage collections of word stems during dictionary processing.

## Definition

```c
static SplitVar *
CopyVar(SplitVar *s, int makedup)
```
## Detailed Description
CopyVar creates a new SplitVar structure and optionally copies the contents from an existing SplitVar. The function handles two scenarios: copying from an existing SplitVar or creating a new empty one. When copying, it can either duplicate the stem strings (deep copy) or just copy the pointers (shallow copy) based on the makedup parameter. This function is essential for managing word variants during spell checking operations where multiple possible stems need to be tracked and manipulated.

## Parameters / Member Variables
- : Source SplitVar structure to copy from (can be NULL for creating empty structure)
- : Boolean flag indicating whether to duplicate stem strings (1) or just copy pointers (0)

## Dependencies
- Functions called/Symbols referenced:
  - palloc (memory allocation)
  - pstrdup (string duplication when makedup is true)
  - SplitVar (structure type)
- Called from (representative examples):
  - SplitToVariants (at src/backend/tsearch/spell.c:2393, 2431)

## Notes and Other Information
- When s is NULL, creates an empty SplitVar with initial capacity of 16 stems
- The function always sets the next pointer to NULL, indicating this creates a standalone node
- Memory management is handled through PostgreSQL's palloc system
- Used specifically in text search spell checking to manage word stem variants during dictionary lookups