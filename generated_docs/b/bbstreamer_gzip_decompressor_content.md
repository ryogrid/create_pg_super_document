# bbstreamer_gzip_decompressor_content

## Location
src/bin/pg_basebackup/bbstreamer_gzip.c: 261 - 315

## Overview
Decompresses input gzip data in chunks and forwards the decompressed output to the next streamer in the chain when the output buffer becomes full.

## Definition
```c
static void bbstreamer_gzip_decompressor_content(bbstreamer *streamer,
                                               bbstreamer_member *member,
                                               const char *data, int len,
                                               bbstreamer_archive_context context)
```

## Detailed Description
This function performs the core decompression work for the gzip decompressor streamer. It processes compressed input data in chunks using zlib's inflate() function. The function continuously decompresses data until all input is consumed, managing the output buffer and forwarding complete chunks to the next streamer in the pipeline. When the output buffer becomes full, it passes the decompressed data to the next streamer via bbstreamer_content() and resets the buffer for continued processing. The function handles the z_stream state management, updating input and output pointers and availability counters as decompression progresses.

## Parameters / Member Variables
- `streamer`: The bbstreamer instance (cast to bbstreamer_gzip_decompressor internally)
- `member`: Archive member information being processed
- `data`: Compressed input data to decompress
- `len`: Length of the input data in bytes
- `context`: Archive context information for the current operation

## Dependencies
- Functions called/Symbols referenced:
  - bbstreamer (base streamer type)
  - bbstreamer_member (member information structure)
  - bbstreamer_archive_context (context information type)
  - bbstreamer_gzip_decompressor (internal decompressor structure)
  - bbstreamer_content (function to forward data to next streamer)
- Called from (representative examples):
  - No direct references found (likely called through function pointer in ops structure)

## Notes and Other Information
- This is a static function, only accessible within the bbstreamer_gzip.c compilation unit
- Uses zlib's inflate() function with Z_NO_FLUSH flag for incremental decompression
- Implements a buffering strategy that forwards data when the output buffer is full
- Handles z_stream state updates for both input consumption and output generation
- Error handling includes checking for Z_STREAM_ERROR and logging decompression failures
- Part of the callback-based streaming architecture where this function is invoked through the ops structure