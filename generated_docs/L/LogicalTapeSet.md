# LogicalTapeSet

## Location
[src/backend/utils/sort/logtape.c:187-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L187-L237)

## Overview
LogicalTapeSet represents a collection of related logical tapes sharing space in a single underlying file, providing the foundation for PostgreSQL's external sorting and disk-based operations with efficient space management and block allocation.

## Definition

```c
struct LogicalTapeSet
{
	BufFile    *pfile;			/* underlying file for whole tape set */
	SharedFileSet *fileset;
	int			worker;			/* worker # if shared, -1 for leader/serial */

	/*
	 * File size tracking.  nBlocksWritten is the size of the underlying file,
	 * in BLCKSZ blocks.  nBlocksAllocated is the number of blocks allocated
	 * by ltsReleaseBlock(), and it is always greater than or equal to
	 * nBlocksWritten.  Blocks between nBlocksAllocated and nBlocksWritten are
	 * blocks that have been allocated for a tape, but have not been written
	 * to the underlying file yet.  nHoleBlocks tracks the total number of
	 * blocks that are in unused holes between worker spaces following BufFile
	 * concatenation.
	 */
	int64		nBlocksAllocated;	/* # of blocks allocated */
	int64		nBlocksWritten; /* # of blocks used in underlying file */
	int64		nHoleBlocks;	/* # of "hole" blocks left */

	/*
	 * We store the numbers of recycled-and-available blocks in freeBlocks[].
	 * When there are no such blocks, we extend the underlying file.
	 *
	 * If forgetFreeSpace is true then any freed blocks are simply forgotten
	 * rather than being remembered in freeBlocks[].  See notes for
	 * LogicalTapeSetForgetFreeSpace().
	 */
	bool		forgetFreeSpace;	/* are we remembering free blocks? */
	int64	   *freeBlocks;		/* resizable array holding minheap */
	int64		nFreeBlocks;	/* # of currently free blocks */
	Size		freeBlocksLen;	/* current allocated length of freeBlocks[] */
	bool		enable_prealloc;	/* preallocate write blocks? */
};
```
## Detailed Description
LogicalTapeSet is the central management structure for PostgreSQL's external sorting system, coordinating multiple logical tapes within a single underlying file. It handles space allocation, block recycling, and provides the infrastructure for efficient disk-based operations when data exceeds available memory.

The tape set employs sophisticated space management through a combination of block allocation tracking and free space recycling. It maintains separate counters for allocated versus written blocks, enabling efficient pre-allocation strategies. The free block management uses a min-heap data structure to efficiently reuse deallocated blocks, reducing file fragmentation.

Support for shared file sets enables parallel operations where multiple worker processes can coordinate their tape operations. The structure also supports "hole" blocks that arise during parallel worker coordination, providing seamless space management across distributed operations.

## Parameters / Member Variables
- `*pfile`: Pointer to the underlying BufFile that stores all tape data
- `*fileset`: Shared file set for coordination in parallel operations
- `worker`: Worker process identifier (-1 for leader/serial operations, >= 0 for worker processes)
- `nBlocksAllocated`: Total number of blocks that have been allocated from the file
- `nBlocksWritten`: Number of blocks actually written to the underlying file
- `nHoleBlocks`: Number of unused "hole" blocks remaining after BufFile concatenation in parallel operations
- `forgetFreeSpace`: Boolean flag controlling whether freed blocks are remembered for reuse
- `*freeBlocks`: Array implementing a min-heap of available recycled block numbers
- `nFreeBlocks`: Current number of blocks available in the free blocks heap
- `freeBlocksLen`: Allocated capacity of the freeBlocks array
- `enable_prealloc`: Boolean flag enabling block preallocation optimization for write operations

## Dependencies
- Functions called/Symbols referenced:
  - [BufFile](../B/BufFile.md)
  - SharedFileSet
  - [LogicalTape](LogicalTape.md)
  - [ltsCreateTape](../l/ltsCreateTape.md)
  - [ltsWriteBlock](../l/ltsWriteBlock.md)
  - [ltsReadBlock](../l/ltsReadBlock.md)
  - [ltsGetBlock](../l/ltsGetBlock.md)
  - [ltsGetFreeBlock](../l/ltsGetFreeBlock.md)
  - [ltsGetPreallocBlock](../l/ltsGetPreallocBlock.md)
  - [ltsReleaseBlock](../l/ltsReleaseBlock.md)
  - [ltsInitReadBuffer](../l/ltsInitReadBuffer.md)
- Called from (representative examples):
  - [LogicalTapeSetCreate](LogicalTapeSetCreate.md)
  - [LogicalTapeSetClose](LogicalTapeSetClose.md)
  - [LogicalTapeCreate](LogicalTapeCreate.md)
  - [LogicalTapeWrite](LogicalTapeWrite.md)
  - [LogicalTapeRewindForRead](LogicalTapeRewindForRead.md)
  - Tuplesort operations
  - [Hash](../H/Hash.md) aggregation

## Notes and Other Information
- Central coordinator for all external sorting operations in PostgreSQL
- Enables efficient sharing of disk space among multiple logical tapes
- Min-heap based free space management minimizes file fragmentation
- Supports both serial and parallel execution modes through SharedFileSet integration
- Block preallocation capability improves performance for write-intensive workloads
- The forgetFreeSpace option allows trading memory usage for simpler space management
- Critical for handling datasets larger than available memory in sorting and aggregation operations
- Seamlessly handles OS file size limits through BufFile abstraction
- Hole block tracking ensures efficient space utilization in parallel scenarios