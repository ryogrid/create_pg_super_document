# TapeBlockTrailer

## Location
[src/backend/utils/sort/logtape.c:95-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L95-L101)

## Overview
TapeBlockTrailer is a structure stored at the end of each BLCKSZ block in PostgreSQL's logical tape implementation, providing linked list functionality for tape blocks.

## Definition

```c
typedef struct TapeBlockTrailer
{
	int64		prev;			/* previous block on this tape, or -1 on first
								 * block */
	int64		next;			/* next block on this tape, or # of valid
								 * bytes on last block (if < 0) */
} TapeBlockTrailer;
```
## Detailed Description
The TapeBlockTrailer structure implements a doubly-linked list mechanism for organizing blocks within logical tapes. Each BLCKSZ-sized block contains this trailer at its end, enabling efficient navigation between blocks and special handling for the first and last blocks in a tape sequence.

The structure uses a clever encoding scheme where the 'next' field serves dual purposes: for intermediate blocks, it points to the next block; for the last block, it stores the number of valid bytes in the block as a negative value, allowing easy identification of the tape's end.

## Parameters / Member Variables
- : Points to the previous block on this tape. Set to -1 for the first block in the tape, indicating no predecessor.
- : For non-terminal blocks, points to the next block on this tape. For the last block, stores the number of valid bytes on the block as a negative value (< 0), serving as an end-of-tape marker.

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - TapeBlockPayloadSize
  - TapeBlockGetTrailer

## Notes and Other Information
- The trailer is positioned at the end of each BLCKSZ block to maximize available payload space
- The negative encoding of valid bytes in the 'next' field provides an elegant way to distinguish terminal blocks
- This structure is fundamental to PostgreSQL's external sorting implementation, enabling efficient disk-based merge operations
- The use of int64 for block pointers allows handling very large tape files