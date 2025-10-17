# bbstreamer_lz4_compressor_finalize

## Location
[src/bin/pg_basebackup/bbstreamer_lz4.c:199-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_lz4.c#L199-L257)

## Overview
Finalizes LZ4 compression by writing the frame footer and flushing remaining data through the streaming chain.

## Definition

```c
static void
bbstreamer_lz4_compressor_finalize(bbstreamer *streamer)
```
## Detailed Description
This function performs end-of-stream processing for LZ4 compression. It calculates the footer boundary requirements, ensures sufficient buffer space, and calls LZ4F_compressEnd to finalize the compression frame and flush any remaining data from the compression context.

The function handles buffer management by forwarding existing compressed data if there isn't enough space for the footer, dynamically resizing the buffer if needed, and ensuring all remaining compressed data is properly forwarded to the next streamer in the chain before finalizing the downstream processing.

## Parameters / Member Variables
- `*streamer`: The LZ4 compressor streamer instance to finalize
## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_compressBound
  - LZ4F_compressEnd
  - [bbstreamer_content](bbstreamer_content.md)
  - [bbstreamer_finalize](bbstreamer_finalize.md)
  - [enlargeStringInfo](../e/enlargeStringInfo.md)
- Called from (representative examples):
  - [bbstreamer](bbstreamer.md) operation table (via function pointer)

## Notes and Other Information
- Must be called to properly close LZ4 compression frames
- Calculates footer space requirements using LZ4F_compressBound with zero input length
- Forwards any buffered compressed data before writing footer
- Calls downstream finalization to complete the processing chain
- Essential for generating valid LZ4 compressed backup files

## Simplified Source

```c
static void
bbstreamer_lz4_compressor_finalize(bbstreamer *streamer)
{
    bbstreamer_lz4_frame *mystreamer = (bbstreamer_lz4_frame *) streamer;
    uint8 *next_out;
    size_t footer_bound, compressed_size, avail_out;

    // Calculate footer space requirements
    footer_bound = LZ4F_compressBound(0, &mystreamer->prefs);

    // Check if buffer has enough space for footer
    if ((mystreamer->base.bbs_buffer.maxlen - mystreamer->bytes_written) < footer_bound) {
        // Forward existing compressed data to next streamer
        bbstreamer_content(mystreamer->base.bbs_next, NULL,
                          mystreamer->base.bbs_buffer.data,
                          mystreamer->bytes_written,
                          BBSTREAMER_UNKNOWN);

        // Enlarge buffer if needed for footer
        if (mystreamer->base.bbs_buffer.maxlen < footer_bound)
            enlargeStringInfo(&mystreamer->base.bbs_buffer, footer_bound);

        avail_out = mystreamer->base.bbs_buffer.maxlen;
        mystreamer->bytes_written = 0;
        next_out = (uint8 *) mystreamer->base.bbs_buffer.data;
    } else {
        next_out = (uint8 *) mystreamer->base.bbs_buffer.data + mystreamer->bytes_written;
        avail_out = mystreamer->base.bbs_buffer.maxlen - mystreamer->bytes_written;
    }

    // Finalize compression and write footer
    compressed_size = LZ4F_compressEnd(mystreamer->cctx, next_out, avail_out, NULL);
    if (LZ4F_isError(compressed_size))
        pg_log_error("could not end lz4 compression: %s",
                     LZ4F_getErrorName(compressed_size));

    mystreamer->bytes_written += compressed_size;

    // Forward final compressed data and finalize chain
    bbstreamer_content(mystreamer->base.bbs_next, NULL,
                      mystreamer->base.bbs_buffer.data,
                      mystreamer->bytes_written,
                      BBSTREAMER_UNKNOWN);

    bbstreamer_finalize(mystreamer->base.bbs_next);
}
```