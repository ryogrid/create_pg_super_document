# CopyLoadRawBuf

## Location
[src/backend/commands/copyfromparse.c:590-649](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L590-L649)

## Overview
CopyLoadRawBuf loads new data from the data source into the raw buffer, managing unprocessed data and updating progress statistics for COPY FROM operations.

## Definition

```c
static void
CopyLoadRawBuf(CopyFromState cstate)
```
## Detailed Description
CopyLoadRawBuf is responsible for refreshing the raw input buffer with new data from the underlying data source during COPY FROM operations. The function implements efficient buffer management by:

1. **Preserving unprocessed data**: Any remaining unprocessed bytes in the buffer are moved to the beginning using memmove() to make room for new data.

2. **Handling buffer relationships**: When no encoding conversion is needed (raw_buf == input_buf), the function ensures both buffer tracking variables stay synchronized.

3. **Loading new data**: Uses CopyGetData() to read additional data from the source, appending it after any preserved unprocessed data.

4. **Progress tracking**: Updates the bytes_processed counter and reports progress through PostgreSQL's progress reporting mechanism.

The function handles the critical transition from having data to reaching EOF by setting the raw_reached_eof flag when no more data can be read.

## Parameters / Member Variables
- : CopyFromState structure containing all COPY operation state including buffers, indices, length counters, and progress tracking

## Dependencies
- Functions called/Symbols referenced:
  - [CopyGetData](CopyGetData.md)
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - PROGRESS_COPY_BYTES_PROCESSED
  - CopyFromState
- Called from (representative examples):
  - [CopyLoadInputBuf](CopyLoadInputBuf.md)
  - [CopyReadBinaryData](CopyReadBinaryData.md)

## Notes and Other Information
- The function includes assertions to verify buffer consistency when raw_buf and input_buf point to the same memory (no transcoding case)
- Buffer management uses memmove() which handles overlapping memory regions safely
- The function always null-terminates the buffer after loading new data for safe string operations
- Progress reporting integration allows users to monitor COPY operations through PostgreSQL's progress views
- RAW_BUF_BYTES macro is used to calculate remaining unprocessed bytes efficiently
- The function sets raw_reached_eof when CopyGetData returns 0 bytes, signaling end of input to calling functions