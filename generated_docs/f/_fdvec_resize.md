# _fdvec_resize

## Location
src/backend/storage/smgr/md.c: 1480 - 1527

## Overview
A static helper function that resizes the array of file descriptors for open segments of a specific fork within a storage manager relation.

## Definition
```c
static void _fdvec_resize(SMgrRelation reln, ForkNumber forknum, int nseg)
```

## Detailed Description
_fdvec_resize manages the dynamic sizing of the md_seg_fds array within a storage manager relation for a specific fork. This array holds file descriptors for all open segments of a relation fork. The function handles three main scenarios: deallocating when no segments are needed (nseg=0), initial allocation when transitioning from 0 segments, and expansion when more segments are required.

A key design decision is that the function never shrinks allocated arrays to avoid memory allocation during mdtruncate() operations, which must be callable within critical sections. This trade-off preserves some unused memory to ensure truncation operations remain allocation-free.

## Parameters / Member Variables
- `reln`: The storage manager relation containing the segment file descriptor array
- `forknum`: The fork number identifying which fork's segment array to resize
- `nseg`: The target number of segments for the array

## Dependencies
- Functions called/Symbols referenced:
  - pfree
  - MemoryContextAlloc
  - repalloc
  - MdCxt (memory context)
  - MdfdVec (segment descriptor structure)
- Called from (representative examples):
  - mdcreate (src/backend/storage/smgr/md.c:236)
  - mdopenfork (src/backend/storage/smgr/md.c:666)
  - mdclose (src/backend/storage/smgr/md.c:705)
  - mdtruncate (src/backend/storage/smgr/md.c:1204)
  - _mdfd_openseg (src/backend/storage/smgr/md.c:1574)

## Notes and Other Information
- The function deliberately avoids shrinking arrays to prevent memory allocation in critical sections
- Uses MdCxt memory context for allocations to ensure proper memory management
- Repalloc operations are not amortized as the cost is minimal compared to file operations
- The md_num_open_segs counter is always updated to reflect the new segment count
- This is an internal function specific to the MD storage manager implementation