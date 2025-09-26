# LogicalTape

## Location
[src/backend/utils/sort/logtape.c:137-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L137-L186)

## Overview
LogicalTape represents a single logical tape within PostgreSQL's external sorting system, providing buffered I/O operations for reading and writing data blocks during disk-based merge operations.

## Definition

```c
struct LogicalTape
{
	LogicalTapeSet *tapeSet;	/* tape set this tape is part of */

	bool		writing;		/* T while in write phase */
	bool		frozen;			/* T if blocks should not be freed when read */
	bool		dirty;			/* does buffer need to be written? */

	/*
	 * Block numbers of the first, current, and next block of the tape.
	 *
	 * The "current" block number is only valid when writing, or reading from
	 * a frozen tape.  (When reading from an unfrozen tape, we use a larger
	 * read buffer that holds multiple blocks, so the "current" block is
	 * ambiguous.)
	 *
	 * When concatenation of worker tape BufFiles is performed, an offset to
	 * the first block in the unified BufFile space is applied during reads.
	 */
	int64		firstBlockNumber;
	int64		curBlockNumber;
	int64		nextBlockNumber;
	int64		offsetBlockNumber;

	/*
	 * Buffer for current data block(s).
	 */
	char	   *buffer;			/* physical buffer (separately palloc'd) */
	int			buffer_size;	/* allocated size of the buffer */
	int			max_size;		/* highest useful, safe buffer_size */
	int			pos;			/* next read/write position in buffer */
	int			nbytes;			/* total # of valid bytes in buffer */

	/*
	 * Preallocated block numbers are held in an array sorted in descending
	 * order; blocks are consumed from the end of the array (lowest block
	 * numbers first).
	 */
	int64	   *prealloc;
	int			nprealloc;		/* number of elements in list */
	int			prealloc_size;	/* number of elements list can hold */
};
```
## Detailed Description
LogicalTape is the core data structure for PostgreSQL's external sorting implementation, enabling efficient disk-based operations when sorting datasets larger than available memory. Each tape operates in either write or read mode, using a sophisticated buffering system to optimize I/O performance.

During the write phase, the tape accumulates data in a buffer until it reaches block size, then writes complete blocks to disk. During the read phase, the tape can employ a larger read buffer containing multiple blocks for improved sequential access performance.

The structure supports both frozen and unfrozen modes: frozen tapes preserve blocks in memory for potential reuse, while unfrozen tapes free blocks after reading to conserve memory. Block preallocation helps reduce fragmentation and improves performance during intensive write operations.

## Parameters / Member Variables
- `*tapeSet`: Pointer to the LogicalTapeSet that contains this tape
- `writing`: Boolean flag indicating if the tape is currently in write mode
- `frozen`: Boolean flag indicating if blocks should be preserved in memory when read
- `dirty`: Boolean flag indicating if the current buffer contains unsaved changes
- `firstBlockNumber`: Block number of the first block in this tape
- `curBlockNumber`: Block number of the currently active block (valid during writing or frozen reading)
- `nextBlockNumber`: Block number of the next block to be written or read
- `offsetBlockNumber`: Offset applied during concatenation of worker tape BufFiles
- `*buffer`: Physical memory buffer for holding current data block(s)
- `buffer_size`: Currently allocated size of the buffer in bytes
- `max_size`: Maximum safe buffer size for optimal performance
- `pos`: Current read/write position within the buffer
- `nbytes`: Total number of valid bytes currently stored in the buffer
- `*prealloc`: Array of preallocated block numbers, sorted in descending order
- `nprealloc`: Number of preallocated blocks currently available
- `prealloc_size`: Maximum capacity of the preallocation array

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTapeSet](LogicalTapeSet.md)
- Called from (representative examples):
  - [LogicalTapeCreate](LogicalTapeCreate.md)
  - [LogicalTapeWrite](LogicalTapeWrite.md)
  - [LogicalTapeRead](LogicalTapeRead.md)
  - [LogicalTapeRewindForRead](LogicalTapeRewindForRead.md)
  - [LogicalTapeFreeze](LogicalTapeFreeze.md)
  - tuplesort operations
  - [Hash](../H/Hash.md) aggregation spilling

## Notes and Other Information
- Central to PostgreSQL's external sorting algorithm, enabling sorts larger than available memory
- Uses sophisticated buffering strategies: single-block buffers during writing, multi-block buffers during reading
- Block preallocation mechanism reduces disk fragmentation and improves write performance
- Supports both sequential and random access patterns through seek operations
- The frozen/unfrozen distinction allows memory optimization during multi-pass algorithms
- Widely used in tuplesort, hash aggregation spilling, and other disk-based operations
- Buffer size adaptation allows performance tuning based on available memory and workload characteristics