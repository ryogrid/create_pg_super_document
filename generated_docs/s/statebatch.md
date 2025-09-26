# statebatch

## Location
[src/include/regex/regguts.h:337-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/regex/regguts.h#L337-L342)

## Overview
The `statebatch` structure is used for bulk allocation of state structures in PostgreSQL's regular expression engine, providing efficient memory management for NFA states similar to how `arcbatch` manages arcs.

## Definition
```c
struct statebatch
{                                       /* for bulk allocation of states */
    struct statebatch *next;            /* chain link */
    size_t          nstates;            /* number of states allocated in this batch */
    struct state    s[FLEXIBLE_ARRAY_MEMBER];
};
```

## Detailed Description
The `statebatch` structure implements a memory pool mechanism for efficiently allocating multiple state structures at once. This approach reduces memory fragmentation and allocation overhead when creating many states for regular expression NFAs. The structure uses a linked list design where each batch can hold a variable number of state structures, with the actual states stored in a flexible array member at the end of the structure.

## Parameters / Member Variables
- `next`: Pointer to the next statebatch in the chain, forming a linked list of batches
- `nstates`: The number of state structures allocated in this particular batch
- `s`: Flexible array member containing the actual state structures

## Dependencies
- Functions called/Symbols referenced:
  - `state` (struct state for the flexible array member)
  - `FLEXIBLE_ARRAY_MEMBER` (macro for flexible array declaration)
- Called from (representative examples):
  - `freenfa` (for cleanup and deallocation of state batches)
  - `newstate` (for state allocation from batches)
  - `STATEBATCHSIZE` (macro that calculates batch size)

## Notes and Other Information
- The structure is part of PostgreSQL's regex engine located in src/include/regex/regguts.h
- Uses the FLEXIBLE_ARRAY_MEMBER technique for variable-length allocation
- The STATEBATCHSIZE macro calculates the total size needed for a batch with n states
- This bulk allocation strategy improves performance when creating large NFAs with many states
- Memory is managed through a chain of batches, allowing for efficient allocation and cleanup
- Mirrors the design pattern of `arcbatch` but for state structures instead of arc structures