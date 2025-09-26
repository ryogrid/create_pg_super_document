# ltsInitReadBuffer

## Location
src/backend/utils/sort/logtape.c: 522 - 555

## Overview
Lazily allocates and initializes the read buffer for a LogicalTape to avoid waste when many tapes are open simultaneously but not all are actively being read between rewinding and reading operations.

## Definition
```c
static void ltsInitReadBuffer(LogicalTape *lt)
```

## Detailed Description
The `ltsInitReadBuffer` function performs lazy initialization of a LogicalTape's read buffer, which is a memory optimization strategy. Instead of allocating read buffers for all tapes when they are created, this function defers buffer allocation until the tape is actually needed for reading operations. This approach prevents memory waste in scenarios where many logical tapes exist but only a subset are actively used.

The function allocates memory for the buffer using `palloc`, sets up the initial reading position, and calls `ltsReadFillBuffer` to populate the buffer with the first block of data from the tape. If the tape is empty, the buffer is properly reset to reflect this state.

## Parameters / Member Variables
- `lt`: Pointer to the LogicalTape structure for which to initialize the read buffer. The tape must have a valid `buffer_size` greater than 0.

## Dependencies
- Functions called/Symbols referenced:
  - palloc (memory allocation)
  - ltsReadFillBuffer (fills buffer with data from tape)
  - LogicalTape (structure type)
- Called from (representative examples):
  - LogicalTapeRead (when reading from tape)
  - LogicalTapeBackspace (when positioning backward)
  - LogicalTapeSeek (when seeking to position)
  - LogicalTapeTell (when querying tape position)

## Notes and Other Information
- This is a static function, only accessible within the logtape.c module
- The function includes an assertion that `buffer_size > 0` to ensure valid buffer allocation
- The lazy initialization pattern is particularly beneficial for external sort operations where many temporary tapes may be created but not all accessed
- The function sets initial reading state by resetting position (`pos`) and byte count (`nbytes`) to 0, then positioning at the first block
- Memory is allocated from PostgreSQL's memory context system using `palloc`