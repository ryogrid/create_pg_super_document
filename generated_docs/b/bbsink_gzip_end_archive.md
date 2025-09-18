# bbsink_gzip_end_archive

## Location
src/backend/backup/basebackup_gzip.c: 225 - 277

## Overview
Finalizes the gzip compression process by flushing any remaining data from zlib's internal buffers and signaling the end of the current archive to the next sink in the chain.

## Definition
```c
static void bbsink_gzip_end_archive(bbsink *sink)
```

## Detailed Description
This function completes the compression of an archive by ensuring all buffered data within zlib's internal structures is flushed out and forwarded to the successor sink. It performs the final compression step using Z_FINISH mode, which tells zlib that no more input data will be provided and forces it to output any remaining compressed data.

The function operates in a loop similar to bbsink_gzip_archive_contents(), but with key differences:
- Sets avail_in to 0 to indicate no more input data is available
- Uses Z_FINISH flag instead of Z_NO_FLUSH to complete the compression stream
- Continues until deflate() produces no more output (bytes_written == 0)
- Forwards any accumulated output to the next sink
- Calls bbsink_forward_end_archive() to notify the entire sink chain that the archive has ended

This ensures that the gzip stream is properly terminated and all compressed data is delivered to the next sink before the archive processing concludes.

## Parameters / Member Variables
- `sink`: The bbsink structure representing this gzip compression sink

## Dependencies
- Functions called/Symbols referenced:
  - deflate (zlib compression function with Z_FINISH)
  - elog (error logging)
  - bbsink_archive_contents (forwards final compressed data to next sink)
  - bbsink_forward_end_archive (notifies sink chain of archive completion)
  - Assert (assertion checking)
- Called from (representative examples):
  - Used as callback function in bbsink_gzip_ops structure

## Notes and Other Information
- This is a static function, only accessible within the compilation unit
- Uses Z_FINISH flag to force zlib to output all remaining compressed data
- Continues processing until deflate() indicates completion (no more output produced)
- Essential for proper gzip stream termination and data integrity
- Forwards end-of-archive notification to the entire sink chain via bbsink_forward_end_archive()
- Handles any final compressed data that may have been buffered internally by zlib
- The loop terminates when bytes_written remains 0, indicating deflate() has no more output
- Ensures complete data delivery before archive processing concludes