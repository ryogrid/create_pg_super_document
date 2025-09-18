# hashbuildempty

## Location
src/backend/access/hash/hash.c: 201 - 209

## Overview
Builds an empty hash index in the initialization fork for use during database initialization or recovery.

## Definition
```c
void hashbuildempty(Relation index)
```

## Detailed Description
The hashbuildempty function creates the basic structure of a hash index without any data tuples. This function is specifically used to build empty hash indexes in the initialization fork (INIT_FORKNUM), which is used during database bootstrap, recovery processes, or when creating unlogged indexes.

The function is a simple wrapper around _hash_init, calling it with 0 tuples to indicate that no initial bucket sizing based on data volume is needed. This creates the minimal hash index structure with just the metadata page and initial bucket structure.

## Parameters / Member Variables
- `index`: The hash index relation to initialize as empty

## Dependencies
- Functions called/Symbols referenced:
  - _hash_init
  - INIT_FORKNUM (constant for initialization fork)
- Called from:
  - hashhandler (as amroutine->ambuildempty callback)
  - Database initialization and recovery systems

## Notes and Other Information
- Used specifically for initialization fork operations, not regular index building
- Creates minimal index structure without data-driven bucket sizing
- Essential for database bootstrap and recovery scenarios
- Much simpler than hashbuild since no heap scanning or tuple processing is required
- The empty index can later be populated through normal insert operations