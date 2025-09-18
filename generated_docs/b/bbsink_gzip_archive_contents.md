# bbsink_gzip_archive_contents

## Location
src/backend/backup/basebackup_gzip.c: 167 - 224

## Overview
Compresses input data and forwards compressed output to the next sink, handling the continuous compression of archive contents with proper buffer management.

## Definition
```c
static void bbsink_gzip_archive_contents(bbsink *sink, size_t len)
```

## Detailed Description
This function implements the core compression logic for archive contents. It takes input data from the sink's buffer and compresses it using zlib's deflate() function, managing both input and output buffers carefully.

The function operates in a loop, continuously compressing data until all input is processed. It handles the scenario where compressed output may be smaller or larger than input, and manages partial fills of the output buffer. When the output buffer becomes full, it forwards the compressed data to the next sink in the chain and resets the buffer position.

Key aspects of the compression process:
- Uses Z_NO_FLUSH mode to allow zlib to optimize compression across multiple calls
- Tracks bytes_written to manage output buffer position
- Handles partial compression where not all input may be processed in one deflate() call
- Forwards full output buffers to the next sink immediately
- Preserves unwritten compressed data in the output buffer for subsequent calls

## Parameters / Member Variables
- `sink`: The bbsink structure representing this gzip compression sink
- `len`: The number of bytes of input data to compress from the sink's buffer

## Dependencies
- Functions called/Symbols referenced:
  - deflate (zlib compression function)
  - elog (error logging)
  - bbsink_archive_contents (forwards compressed data to next sink)
  - Assert (assertion checking)
- Called from (representative examples):
  - Used as callback function in bbsink_gzip_ops structure

## Notes and Other Information
- This is a static function, only accessible within the compilation unit
- Uses Z_NO_FLUSH to allow optimal compression across multiple function calls
- Compressed data may not be immediately forwarded - it accumulates in the output buffer
- Handles the case where deflate() consumes only part of the input in one call
- Output buffer management ensures no data loss between compression calls
- Z_STREAM_ERROR from deflate() indicates programming errors and triggers elog(ERROR)
- The function may be called multiple times with different input chunks for the same archive
- Final compressed data may not be sent until bbsink_gzip_end_archive() is called