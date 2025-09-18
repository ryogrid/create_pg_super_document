# BumpContextCreate

## Location
src/backend/utils/mmgr/bump.c: 131 - 242

## Overview
Creates a new Bump memory context, which is a specialized memory allocation context optimized for append-only allocation patterns with efficient bulk deallocation.

## Definition


## Detailed Description
BumpContextCreate initializes a Bump memory context that provides efficient memory allocation for scenarios where memory is primarily allocated sequentially and freed all at once. The context uses a block-based allocation strategy where memory is allocated from contiguous blocks, and when a block is exhausted, a new larger block is allocated. This design is particularly efficient for temporary data structures that grow incrementally and are discarded entirely.

The function performs extensive validation of input parameters, allocates the initial block containing both the context header and block header, initializes the block management structures, and sets up allocation limits based on the maximum block size and chunk constraints.

## Parameters / Member Variables
- : Parent memory context, or NULL if this is a top-level context
- : Name of the context (must be statically allocated string)
- : Minimum size for the initial context allocation
- : Initial size for allocation blocks (must be ≥1024 and MAXALIGNED)
- : Maximum size for allocation blocks (must be ≥initBlockSize and ≤MEMORYCHUNK_MAX_BLOCKOFFSET)

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - MemoryContextStats
  - MemoryContextCreate
  - dlist_init
  - dlist_push_head
  - KeeperBlock
  - BumpBlockInit
  - StaticAssertDecl
  - AllocHugeSizeIsValid
- Called from (representative examples):
  - TidStoreCreateLocal
  - tuplesort_begin_batch

## Notes and Other Information
- The initial block layout is unique compared to other Bump blocks as it starts with the context header followed by the block header
- The function calculates allocChunkLimit to ensure efficient space utilization, limiting chunk sizes to fit at least Bump_CHUNK_FRACTION chunks per maximum block
- All size parameters must be MAXALIGNED and the function enforces minimum sizes and maximum limits for memory safety
- The context uses a doubly-linked list to manage blocks for efficient traversal during reset operations