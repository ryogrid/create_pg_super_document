# BlockRefTableGetEntry

## Location
[src/common/blkreftable.c:340-368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L340-L368)

## Overview
Retrieves an entry from a block reference table for a specific relation fork, returning the entry and its associated limit block value.

## Definition
```c
BlockRefTableEntry *BlockRefTableGetEntry(BlockRefTable *brtab, 
                                          const RelFileLocator *rlocator,
                                          ForkNumber forknum, 
                                          BlockNumber *limit_block)
```

## Detailed Description
This function performs a lookup in the block reference table to find an entry corresponding to a specific relation fork. If the entry exists, the function returns a pointer to the BlockRefTableEntry and sets the output parameter to contain the limit block value from that entry. If no entry exists for the specified relation fork, the function returns NULL.

The function operates by:
1. Creating a lookup key from the relation locator and fork number
2. Using the hash table lookup function to find the corresponding entry
3. If found, extracting the limit_block value and returning the entry pointer
4. If not found, returning NULL without modifying the limit_block parameter

This is a read-only operation that does not modify the table or create new entries.

## Parameters / Member Variables
- : Pointer to the BlockRefTable to search
- : Pointer to RelFileLocator identifying the specific relation
- : Fork number (main, fsm, vm, etc.) within the relation
- : Output parameter to receive the limit block value (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - : Assertion macro to validate that limit_block is not NULL
  - : Copies the RelFileLocator to the key structure
  - : Performs hash table lookup for the key
- Called from (representative examples):
  - : During incremental backup to determine backup method for files

## Notes and Other Information
- This is a pure lookup function that does not modify the block reference table
- The limit_block parameter must not be NULL, as enforced by an assertion
- The limit_block output parameter is only modified if an entry is found
- Returns NULL when no entry exists for the specified relation fork
- The BlockRefTableKey structure is zero-initialized to ensure consistent padding for hash operations
- This function is commonly used during backup operations to check if a relation fork has modification tracking information
- The returned BlockRefTableEntry pointer should not be freed by the caller, as it belongs to the hash table