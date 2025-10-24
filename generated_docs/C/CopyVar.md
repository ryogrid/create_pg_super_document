# CopyVar

## Location
[src/backend/tsearch/spell.c:2336-2360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2336-L2360)

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
- `*s`: Source SplitVar structure to copy from (can be NULL for creating empty structure)
- `makedup`: Boolean flag indicating whether to duplicate stem strings (1) or just copy pointers (0)
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [pstrdup](../p/pstrdup.md) (string duplication when makedup is true)
  - [SplitVar](../S/SplitVar.md) (structure type)
- Called from (representative examples):
  - [SplitToVariants](../S/SplitToVariants.md) (at src/backend/tsearch/spell.c:2393, 2431)

## Notes and Other Information
- When s is NULL, creates an empty SplitVar with initial capacity of 16 stems
- The function always sets the next pointer to NULL, indicating this creates a standalone node
- Memory management is handled through PostgreSQL's palloc system
- Used specifically in text search spell checking to manage word stem variants during dictionary lookups

## Simplified Source

```c
static SplitVar *
CopyVar(SplitVar *s, int makedup)
{
    // Allocate new SplitVar structure
    SplitVar *v = palloc(sizeof(SplitVar));
    v->next = NULL;

    if (s)
    {
        // Copy from existing SplitVar
        v->lenstem = s->lenstem;
        v->nstem = s->nstem;
        v->stem = palloc(sizeof(char *) * v->lenstem);

        // Copy stem pointers, duplicating strings if requested
        for (int i = 0; i < s->nstem; i++)
            v->stem[i] = makedup ? pstrdup(s->stem[i]) : s->stem[i];
    }
    else
    {
        // Create empty SplitVar with default capacity
        v->lenstem = 16;
        v->nstem = 0;
        v->stem = palloc(sizeof(char *) * v->lenstem);
    }

    return v;
}
```