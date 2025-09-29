# smgrrelease

## Location
[src/backend/storage/smgr/smgr.c:300-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L300-L319)

## Overview
Releases all resources used by an SMgrRelation object while keeping the object itself valid for future use.

## Definition

```c
void
smgrrelease(SMgrRelation reln)
```
## Detailed Description
The  function performs a controlled release of resources associated with an SMgrRelation object without destroying the object itself. It closes all fork files associated with the relation and resets cached block numbers to invalid values. Unlike , this function preserves the SMgrRelation object in the hash table and linked list, allowing it to be reused later. This is useful for freeing up file descriptors and memory while maintaining the relation's metadata structure.

## Parameters / Member Variables
- : Pointer to the SMgrRelation object whose resources should be released. The object remains valid after this operation.

## Dependencies
- Functions called/Symbols referenced:
  - smgrsw[].smgr_close (closes file descriptors for all forks)
  - MAX_FORKNUM (maximum fork number constant)
  - InvalidBlockNumber (constant used to reset cached values)
- Called from (representative examples):
  - [smgrclose](smgrclose.md)
  - [smgrreleaseall](smgrreleaseall.md)
  - [smgrreleaserellocator](smgrreleaserellocator.md)
  - SmgrIsTemp

## Notes and Other Information
- This is a public function (not static), available to other modules
- The SMgrRelation object remains in the hash table and linked list after release
- All cached block numbers are reset to InvalidBlockNumber to ensure data consistency
- The target block number is also reset to InvalidBlockNumber
- This function is typically used during cleanup operations or when file descriptors need to be freed
- Unlike smgrdestroy, this function can be called multiple times safely on the same object

## Simplified Source

```c
// Release all resources used by SMgrRelation object
// Object remains valid after this operation
void smgrrelease(SMgrRelation reln)
{
    // Close all fork files and reset cached block counts
    for (ForkNumber forknum = 0; forknum <= MAX_FORKNUM; forknum++) {
        smgrsw[reln->smgr_which].smgr_close(reln, forknum);
        reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
    }

    // Reset target block number
    reln->smgr_targblock = InvalidBlockNumber;
}
```