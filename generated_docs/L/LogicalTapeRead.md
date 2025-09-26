# LogicalTapeRead

## Location
[src/backend/utils/sort/logtape.c:928-980](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L928-L980)

## Overview
LogicalTapeRead is a function that reads data from a logical tape, providing buffered sequential access to data stored in temporary files during sorting operations.

## Definition

```c
structive read.
	 */
	if (lt->dirty)
	{
		/*
		 * As long as we've filled the buffer at least once, its contents are
		 * entirely defined from valgrind's point of view, even though
		 * contents beyond the current end point may be stale.  But it's
		 * possible - at least in the case of a parallel sort - to sort such
		 * small amount of data that we do not fill the buffer even once. Tell
		 * valgrind that its contents are defined, so it doesn't bleat.
		 */
		VALGRIND_MAKE_MEM_DEFINED(lt->buffer + lt->nbytes,
								  lt->buffer_size - lt->nbytes);

		TapeBlockSetNBytes(lt->buffer, lt->nbytes);
		ltsWriteBlock(lt->tapeSet, lt->curBlockNumber, lt->buffer);
	}
	lt->writing = false;
```
## Detailed Description
LogicalTapeRead performs buffered reading from a logical tape data structure. It ensures the tape is in read mode (not writing), initializes the read buffer if necessary, and then reads the requested amount of data into the provided buffer. The function handles partial reads and EOF conditions gracefully, returning the actual number of bytes read which may be less than requested if EOF is encountered.

The function works by:
1. Verifying the tape is not in writing mode
2. Initializing the read buffer if it hasn't been set up yet
3. Reading data in chunks from the internal buffer
4. Refilling the buffer from storage when needed
5. Copying data to the caller's buffer until the request is satisfied or EOF is reached

## Parameters / Member Variables
- : Pointer to the LogicalTape structure representing the tape to read from
- : Destination buffer where the read data will be stored
- : Number of bytes requested to read

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTape](LogicalTape.md) (structure type)
  - [ltsInitReadBuffer](../l/ltsInitReadBuffer.md) (initializes read buffer)
  - [ltsReadFillBuffer](../l/ltsReadFillBuffer.md) (refills buffer from storage)
- Called from (representative examples):
  - [hashagg_batch_read](../h/hashagg_batch_read.md) (in nodeAgg.c for hash aggregation)
  - [getlen](../g/getlen.md) (in tuplesort.c for tuple sorting)
  - LogicalTapeReadExact (wrapper function for exact reads)

## Notes and Other Information
- Returns the actual number of bytes read, which may be less than requested if EOF is encountered
- Early EOF detection is built into the function's design
- The function maintains internal buffering for efficient I/O operations
- Must not be called on a tape that is currently in writing mode
- Part of PostgreSQL's external sorting infrastructure used during large sort operations