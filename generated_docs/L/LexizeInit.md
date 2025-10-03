# LexizeInit

## Location
[src/backend/tsearch/ts_parse.c:61-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L61-L72)

## Overview
LexizeInit initializes a LexizeData structure for text search parsing operations, setting up the context needed for tokenizing and lexeme processing.

## Definition

```c
static void
LexizeInit(LexizeData *ld, TSConfigCacheEntry *cfg)
```
## Detailed Description
LexizeInit is a static initialization function that prepares a LexizeData structure for text search operations. It sets up the configuration reference and initializes all internal state variables to their default values. The function establishes the foundation for subsequent lexeme processing by clearing all work queues, resetting dictionary tracking, and preparing the data structure for token processing workflows.

The function ensures that all pointer fields are properly nullified and counters are reset, creating a clean slate for text parsing operations. This is essential for consistent behavior across multiple parsing sessions and prevents stale data from affecting new operations.

## Parameters / Member Variables
- `*ld`: Pointer to LexizeData structure to be initialized - contains all state for lexeme processing
- `*cfg`: Pointer to TSConfigCacheEntry containing text search configuration settings
## Dependencies
- Functions called/Symbols referenced:
  - LexizeData (structure type)
  - [TSConfigCacheEntry](../T/TSConfigCacheEntry.md) (structure type)
  - InvalidOid (constant)
- Called from (representative examples):
  - [parsetext](../p/parsetext.md)
  - [hlparsetext](../h/hlparsetext.md)

## Notes and Other Information
- This is a static function, only accessible within ts_parse.c
- Critical initialization step before any text parsing operations
- Ensures clean state by zeroing all tracking variables and pointers
- Part of PostgreSQL's full-text search subsystem infrastructure