# AllocSetContext

## Location
src/backend/utils/mmgr/aset.c: 152 - 165

## Overview
AllocSetContext is PostgreSQL's standard implementation of MemoryContext, providing efficient memory allocation and management with support for block-based allocation and free lists.

## Definition
```c
typedef struct AllocSetContext
{
    MemoryContextData header;        /* Standard memory-context fields */
    /* Info about storage allocated in this context: */
    AllocBlock      blocks;          /* head of list of blocks in this set */
    MemoryChunk    *freelist[ALLOCSET_NUM_FREELISTS]; /* free chunk lists */
    /* Allocation parameters for this context: */
    uint32          initBlockSize;   /* initial block size */
    uint32          maxBlockSize;    /* maximum block size */
    uint32          nextBlockSize;   /* next block size to allocate */
    uint32          allocChunkLimit; /* effective chunk size limit */
    /* freelist this context could be put in, or -1 if not a candidate: */
    int             freeListIndex;   /* index in context_freelists[], or -1 */
} AllocSetContext;
```

## Detailed Description
AllocSetContext is the core data structure implementing PostgreSQL's allocation set memory management system. It extends the basic MemoryContext interface with sophisticated memory management features including block-based allocation, free list optimization, and dynamic block sizing. The structure maintains a linked list of memory blocks and per-size free lists to enable efficient allocation and reuse of memory chunks. The context tracks allocation parameters that control how new blocks are allocated and sized, implementing a strategy that balances memory efficiency with allocation performance. The isReset flag in the header indicates whether there's nothing for AllocSetReset to do, which is distinct from being physically or logically empty.

## Parameters / Member Variables
- `header`: Standard MemoryContextData fields providing the base memory context interface
- `blocks`: Head pointer to the linked list of AllocBlock structures in this allocation set
- `freelist`: Array of free chunk lists, indexed by size class for efficient chunk reuse
- `initBlockSize`: Initial size for the first block allocated in this context
- `maxBlockSize`: Maximum allowed size for blocks in this context
- `nextBlockSize`: Size to use for the next block allocation (grows dynamically)
- `allocChunkLimit`: Effective limit on chunk size for this context
- `freeListIndex`: Index in the global context_freelists array, or -1 if not eligible for context reuse

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextData](../M/MemoryContextData.md) (base memory context structure)
  - [AllocBlock](AllocBlock.md) (memory block pointer type)
  - MemoryChunk (individual memory chunk structure)
  - ALLOCSET_NUM_FREELISTS (constant defining number of free lists)
- Called from (representative examples):
  - [AllocSetContextCreateInternal](AllocSetContextCreateInternal.md) (creates and initializes new allocation contexts)
  - [AllocSetDelete](AllocSetDelete.md) (destroys allocation contexts)
  - [AllocSetStats](AllocSetStats.md) (reports allocation statistics)
  - AllocSetIsValid (validates allocation context integrity)
  - KeeperBlock (manages the keeper block for the context)

## Notes and Other Information
- Central to PostgreSQL's custom memory management system that provides better performance than standard malloc/free
- The free list optimization reduces allocation overhead by reusing chunks of common sizes
- Block size grows dynamically to reduce allocation overhead for large memory consumers
- The keeper block concept ensures a minimum allocation remains even when the context is reset
- Context recycling through freeListIndex helps reduce the overhead of creating/destroying contexts
- The isReset flag optimization avoids unnecessary work during context resets
- Used extensively throughout PostgreSQL for managing memory in query execution, parsing, and other subsystems