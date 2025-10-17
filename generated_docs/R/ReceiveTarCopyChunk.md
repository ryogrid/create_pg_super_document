# ReceiveTarCopyChunk

## Location
[src/bin/pg_basebackup/pg_basebackup.c:1662-1677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L1662-L1677)

## Overview
ReceiveTarCopyChunk is a callback function used by ReceiveTarFile to process individual chunks of tar-format data received from the PostgreSQL server during base backup operations.

## Definition
```c
static void ReceiveTarCopyChunk(size_t r, char *copybuf, void *callback_data)
```

## Detailed Description
This function serves as a data processing callback that handles individual chunks of tar data as they arrive from the server during a base backup operation. It is designed to be called repeatedly by the ReceiveCopyData infrastructure, processing data in streaming fashion rather than loading entire files into memory.

The function takes the received data chunk and forwards it to the backup streamer system using bbstreamer_content. The BBSTREAMER_UNKNOWN flag indicates that the chunk type is not specifically identified, allowing the streaming infrastructure to handle the data appropriately based on its internal state.

Additionally, the function maintains global progress tracking by updating the totaldone counter and calling progress_report to provide user feedback about backup progress. This ensures users receive regular updates about the backup operation's status.

## Parameters / Member Variables
- `r`: Size of the received data chunk in bytes
- `copybuf`: Pointer to the buffer containing the received tar data chunk
- `callback_data`: Void pointer to callback-specific data, cast to WriteTarState* containing streamer and tablespace information

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_content](../b/bbstreamer_content.md)
  - [progress_report](../p/progress_report.md)
  - [WriteTarState](../W/WriteTarState.md) (type cast)
  - BBSTREAMER_UNKNOWN (constant)
- Called from (representative examples):
  - [ReceiveTarFile](ReceiveTarFile.md) (via ReceiveCopyData callback mechanism)

## Notes and Other Information
- This is a static function, only accessible within the pg_basebackup.c compilation unit
- Designed as a callback function compatible with the ReceiveCopyData infrastructure
- Updates global progress tracking (totaldone variable) for user feedback
- Uses the bbstreamer API for flexible output processing and compression handling
- The function is stateless except for the callback_data parameter, making it suitable for repeated invocation
- Progress reporting is called for every chunk, providing responsive user feedback during large backup operations

## Simplified Source

```c
static void
ReceiveTarCopyChunk(size_t r, char *copybuf, void *callback_data)
{
    WriteTarState *state = callback_data;

    // Forward chunk to backup streamer
    bbstreamer_content(state->streamer, NULL, copybuf, r, BBSTREAMER_UNKNOWN);

    // Update progress tracking
    totaldone += r;
    progress_report(state->tablespacenum, false, false);
}
```