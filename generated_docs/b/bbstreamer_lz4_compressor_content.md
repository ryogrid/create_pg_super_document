# bbstreamer_lz4_compressor_content

## Location
[src/bin/pg_basebackup/bbstreamer_lz4.c:116-198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_lz4.c#L116-L198)

## Overview
Compresses input data using LZ4 compression and forwards it through the backup streaming chain.

## Definition

```c
static void
bbstreamer_lz4_compressor_content(bbstreamer *streamer,
								  bbstreamer_member *member,
								  const char *data, int len,
								  bbstreamer_archive_context context)
```
## Detailed Description
This function handles the core LZ4 compression operation for backup data streams. It manages the compression process by writing the LZ4 header on first invocation, calculating compression bounds to ensure sufficient output buffer capacity, and performing the actual data compression using LZ4F_compressUpdate.

The function implements a buffering strategy where it forwards compressed data to the next streamer when the output buffer approaches capacity limits. It dynamically resizes buffers when needed and maintains compression state across multiple invocations.

## Parameters / Member Variables
- `*streamer`: The LZ4 compressor streamer instance
- `*member`: Information about the current archive member being processed
- `*data`: Input data buffer to compress
- `len`: Length of input data in bytes
- `context`: Archive context information
## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_compressBegin
  - LZ4F_compressUpdate  
  - LZ4F_compressBound
  - [bbstreamer_content](bbstreamer_content.md)
  - [enlargeStringInfo](../e/enlargeStringInfo.md)
- Called from (representative examples):
  - [bbstreamer](bbstreamer.md) operation table (via function pointer)

## Notes and Other Information
- Writes LZ4 frame header before processing first data chunk
- Uses compression bounds calculation to prevent buffer overflows
- Forwards data to next streamer when output buffer capacity is insufficient
- Dynamically resizes output buffer if needed to accommodate compression bounds
- Part of the streaming compression pipeline for PostgreSQL backups

## Simplified Source

```c
static void
bbstreamer_lz4_compressor_content(bbstreamer *streamer,
                                  bbstreamer_member *member,
                                  const char *data, int len,
                                  bbstreamer_archive_context context)
{
    bbstreamer_lz4_frame *mystreamer = (bbstreamer_lz4_frame *) streamer;
    uint8 *next_in = (uint8 *) data;
    uint8 *next_out;
    size_t out_bound, compressed_size, avail_out;

    // Write LZ4 header on first invocation
    if (!mystreamer->header_written) {
        compressed_size = LZ4F_compressBegin(mystreamer->cctx,
                                            (uint8 *) mystreamer->base.bbs_buffer.data,
                                            mystreamer->base.bbs_buffer.maxlen,
                                            &mystreamer->prefs);
        if (LZ4F_isError(compressed_size))
            pg_log_error("could not write lz4 header: %s",
                         LZ4F_getErrorName(compressed_size));

        mystreamer->bytes_written += compressed_size;
        mystreamer->header_written = true;
    }

    // Calculate output buffer position and available space
    next_out = (uint8 *) mystreamer->base.bbs_buffer.data + mystreamer->bytes_written;
    avail_out = mystreamer->base.bbs_buffer.maxlen - mystreamer->bytes_written;

    // Check if buffer has enough space for compression
    out_bound = LZ4F_compressBound(len, &mystreamer->prefs);
    if (avail_out < out_bound) {
        // Forward current buffer contents to next streamer
        bbstreamer_content(mystreamer->base.bbs_next, member,
                          mystreamer->base.bbs_buffer.data,
                          mystreamer->bytes_written, context);

        // Enlarge buffer if needed and reset position
        if (mystreamer->base.bbs_buffer.maxlen < out_bound)
            enlargeStringInfo(&mystreamer->base.bbs_buffer, out_bound);

        avail_out = mystreamer->base.bbs_buffer.maxlen;
        mystreamer->bytes_written = 0;
        next_out = (uint8 *) mystreamer->base.bbs_buffer.data;
    }

    // Compress the input data
    compressed_size = LZ4F_compressUpdate(mystreamer->cctx,
                                         next_out, avail_out,
                                         next_in, len, NULL);
    if (LZ4F_isError(compressed_size))
        pg_log_error("could not compress data: %s",
                     LZ4F_getErrorName(compressed_size));

    mystreamer->bytes_written += compressed_size;
}
```