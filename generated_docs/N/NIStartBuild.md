# NIStartBuild

## Location
[src/backend/tsearch/spell.c:88-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L88-L102)

## Overview
NIStartBuild initializes the construction of an ISpell dictionary by setting up the required memory context for dictionary building operations.

## Definition

```c
void
NIStartBuild(IspellDict *Conf)
```
## Detailed Description
This function prepares for constructing an ISpell dictionary by creating a temporary memory context specifically for dictionary initialization. The function assumes that the IspellDict structure passed to it has been zeroed when allocated. The created context is a child of CurTransactionContext, ensuring automatic cleanup on transaction abort or error conditions.

The buildCxt member of the IspellDict structure is set to a newly created AllocSet memory context with default sizing parameters. This context will be used throughout the dictionary building process to manage temporary allocations.

## Parameters / Member Variables
- : Pointer to an IspellDict structure that will be initialized for dictionary building. The structure is expected to be pre-zeroed.

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - ALLOCSET_DEFAULT_SIZES (macro)
  - IspellDict (struct type)
- Called from (representative examples):
  - [dispell_init](../d/dispell_init.md)

## Notes and Other Information
- The function is part of PostgreSQL's full-text search functionality, specifically for ISpell dictionary support
- The memory context created is automatically cleaned up if an error occurs during dictionary building
- This function must be called before any dictionary building operations
- The IspellDict structure should be zero-initialized before calling this function