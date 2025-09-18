# LogicalTape

## Location
[src/backend/utils/sort/logtape.c:137-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L137-L186)

## Overview
LogicalTape represents a single logical tape within PostgreSQL's external sorting system, providing buffered I/O operations for reading and writing data blocks during disk-based merge operations.

## Definition


## Detailed Description
LogicalTape is the core data structure for PostgreSQL's external sorting implementation, enabling efficient disk-based operations when sorting datasets larger than available memory. Each tape operates in either write or read mode, using a sophisticated buffering system to optimize I/O performance.

During the write phase, the tape accumulates data in a buffer until it reaches block size, then writes complete blocks to disk. During the read phase, the tape can employ a larger read buffer containing multiple blocks for improved sequential access performance.

The structure supports both frozen and unfrozen modes: frozen tapes preserve blocks in memory for potential reuse, while unfrozen tapes free blocks after reading to conserve memory. Block preallocation helps reduce fragmentation and improves performance during intensive write operations.

## Parameters / Member Variables
- : Pointer to the LogicalTapeSet that contains this tape
- : Boolean flag indicating if the tape is currently in write mode
- : Boolean flag indicating if blocks should be preserved in memory when read
- : Boolean flag indicating if the current buffer contains unsaved changes
- : Block number of the first block in this tape
- : Block number of the currently active block (valid during writing or frozen reading)
- : Block number of the next block to be written or read
- : Offset applied during concatenation of worker tape BufFiles
- : Physical memory buffer for holding current data block(s)
- : Currently allocated size of the buffer in bytes
- : Maximum safe buffer size for optimal performance
- : Current read/write position within the buffer
- : Total number of valid bytes currently stored in the buffer
- : Array of preallocated block numbers, sorted in descending order
- : Number of preallocated blocks currently available
- : Maximum capacity of the preallocation array

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTapeSet](LogicalTapeSet.md)
- Called from (representative examples):
  - LogicalTapeCreate
  - LogicalTapeWrite
  - LogicalTapeRead
  - LogicalTapeRewindForRead
  - LogicalTapeFreeze
  - tuplesort operations
  - Hash aggregation spilling

## Notes and Other Information
- Central to PostgreSQL's external sorting algorithm, enabling sorts larger than available memory
- Uses sophisticated buffering strategies: single-block buffers during writing, multi-block buffers during reading
- Block preallocation mechanism reduces disk fragmentation and improves write performance
- Supports both sequential and random access patterns through seek operations
- The frozen/unfrozen distinction allows memory optimization during multi-pass algorithms
- Widely used in tuplesort, hash aggregation spilling, and other disk-based operations
- Buffer size adaptation allows performance tuning based on available memory and workload characteristics