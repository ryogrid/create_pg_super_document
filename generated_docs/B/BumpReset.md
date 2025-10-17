# BumpReset

## Location
[src/backend/utils/mmgr/bump.c:243-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L243-L277)

## Overview
Resets a Bump memory context by freeing all allocated memory blocks except the keeper block, effectively returning the context to its initial empty state.

## Definition

```c
void
BumpReset(MemoryContext context)
```
## Detailed Description
BumpReset performs a bulk deallocation operation that is the primary strength of the Bump allocation strategy. The function iterates through all blocks in the context's doubly-linked list, freeing non-keeper blocks entirely and marking the keeper block as empty for reuse. This approach allows for very efficient cleanup of all allocated memory in O(n) time where n is the number of blocks, rather than having to track and free individual allocations.

After freeing the blocks, the function resets the nextBlockSize to the initial block size, preparing the context for future allocation cycles. The function includes debugging assertions to verify the context's integrity both before and after the reset operation.

## Parameters / Member Variables
- `context`: The Bump memory context to reset (cast internally to BumpContext*)

## Dependencies
- Functions called/Symbols referenced:
  - BumpIsValid
  - [BumpCheck](BumpCheck.md) (in MEMORY_CONTEXT_CHECKING builds)
  - dlist_foreach_modify
  - dlist_container
  - IsKeeperBlock
  - [BumpBlockMarkEmpty](BumpBlockMarkEmpty.md)
  - [BumpBlockFree](BumpBlockFree.md)
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [dlist_has_next](../d/dlist_has_next.md)
  - [dlist_head_node](../d/dlist_head_node.md)
- Called from (representative examples):
  - [BumpDelete](BumpDelete.md)
  - BOGUS_MCTX (via function pointer)

## Notes and Other Information
- The reset operation preserves the keeper block which contains the context header, making the context immediately reusable
- Memory context checking is performed if MEMORY_CONTEXT_CHECKING is defined, adding validation overhead in debug builds
- The function ensures exactly one block remains after reset (the keeper block) through assertions
- This operation is much more efficient than individual chunk deallocation, making Bump contexts ideal for scenarios with bulk allocation/deallocation patterns

## Simplified Source

```c
void
BumpReset(MemoryContext context)
{
    BumpContext *set = (BumpContext *) context;
    dlist_mutable_iter miter;

    Assert(BumpIsValid(set));

    // Check for corruption before proceeding (debug builds only)
#ifdef MEMORY_CONTEXT_CHECKING
    BumpCheck(context);
#endif

    // Walk through all blocks and reset them
    dlist_foreach_modify(miter, &set->blocks)
    {
        BumpBlock *block = dlist_container(BumpBlock, node, miter.cur);

        if (IsKeeperBlock(set, block))
            BumpBlockMarkEmpty(block);  // Keep keeper block, just mark it empty
        else
            BumpBlockFree(set, block);  // Free non-keeper blocks completely
    }

    // Reset block size sequence for future allocations
    set->nextBlockSize = set->initBlockSize;

    // Verify we have exactly one block remaining (the keeper block)
    Assert(!dlist_is_empty(&set->blocks));
    Assert(!dlist_has_next(&set->blocks, dlist_head_node(&set->blocks)));
}
```