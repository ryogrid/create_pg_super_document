# BumpIsEmpty

## Location
[src/backend/utils/mmgr/bump.c:660-687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L660-L687)

## Overview
BumpIsEmpty checks whether a BumpContext contains any allocated memory chunks by iterating through all blocks and checking if they contain allocated data.

## Definition


## Detailed Description
This function determines if a Bump memory context is empty of any allocated space. It works by iterating through all blocks in the context using a doubly-linked list iterator and checking each block to see if it contains any allocated chunks. The function returns true only if all blocks in the context are empty, meaning no memory has been allocated or all allocated memory has been conceptually freed (though the Bump allocator doesn't actually free individual chunks, only resets entire contexts).

## Parameters / Member Variables
- `context`: The MemoryContext to check (cast internally to BumpContext)

## Dependencies
- Functions called/Symbols referenced:
  - BumpIsValid (validation check)
  - dlist_foreach (list iteration)
  - dlist_container (container extraction)
  - [BumpBlockIsEmpty](BumpBlockIsEmpty.md) (block emptiness check)
- Called from (representative examples):
  - BOGUS_MCTX (via function pointer table)
  - Memory context interface functions

## Notes and Other Information
- Uses assertion to validate the context is a proper BumpContext
- Iterates through the doubly-linked list of blocks using dlist_foreach
- Returns false as soon as any non-empty block is found (early termination optimization)
- Part of the standard MemoryContext interface for introspection
- Located in src/backend/utils/mmgr/bump.c:660-687