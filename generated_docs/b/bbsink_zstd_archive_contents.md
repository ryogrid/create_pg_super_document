# bbsink_zstd_archive_contents

## Location
[src/backend/backup/basebackup_zstd.c:193-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_zstd.c#L193-L234)

## Overview
Compresses input data using zstd streaming compression, managing output buffer space and passing compressed data to the next sink in the chain when buffers fill up.

## Definition

```c
static void
bbsink_zstd_archive_contents(bbsink *sink, size_t len)
```
## Detailed Description
This function performs the core zstd compression work by processing input data through streaming compression. It sets up zstd input buffer structures, calculates compression bounds to manage output buffer space, and compresses data in chunks. When the output buffer doesn't have sufficient space for the next compression operation, it flushes the current compressed data to the next sink and resets the output buffer. The function handles compression errors and ensures all input data is processed, though compressed output may be buffered and not immediately sent downstream.

## Parameters / Member Variables
- `*sink`: Pointer to the bbsink structure (cast to bbsink_zstd internally) that contains compression context and buffers
- `len`: Number of bytes of input data to compress from the sink's buffer
## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_compressBound (calculates maximum space needed for compression)
  - ZSTD_compressStream2 (performs streaming compression)
  - ZSTD_isError (checks for compression errors) 
  - ZSTD_getErrorName (gets error description)
  - [bbsink_archive_contents](bbsink_archive_contents.md) (sends compressed data to next sink)
  - elog (error logging)
- Called from (representative examples):
  - Through bbsink_zstd_ops function pointer table

## Notes and Other Information
- Uses streaming compression with ZSTD_e_continue mode to allow incremental processing
- Manages output buffer dynamically, flushing when space is insufficient for next compression
- Input may be fully consumed without filling output buffer due to compression efficiency
- Compressed data may be buffered and not sent downstream until later calls or end_archive
- Calculates compression bound to determine if output buffer has sufficient space
- Resets output buffer to next sink's buffer after flushing compressed data
- Function is static and called through the bbsink operations table
- Error handling reports specific zstd compression errors with descriptive messages

## Simplified Source

```c
static void bbsink_zstd_archive_contents(bbsink *sink, size_t len) {
    bbsink_zstd *mysink = (bbsink_zstd *) sink;
    ZSTD_inBuffer inBuf = {mysink->base.bbs_buffer, len, 0};

    // Process all input data through compression
    while (inBuf.pos < inBuf.size) {
        size_t yet_to_flush;
        size_t max_needed = ZSTD_compressBound(inBuf.size - inBuf.pos);

        // Check if output buffer has enough space for compression
        if (mysink->zstd_outBuf.size - mysink->zstd_outBuf.pos < max_needed) {
            // Flush current compressed data to next sink
            bbsink_archive_contents(mysink->base.bbs_next,
                                   mysink->zstd_outBuf.pos);

            // Reset output buffer
            mysink->zstd_outBuf.dst = mysink->base.bbs_next->bbs_buffer;
            mysink->zstd_outBuf.size = mysink->base.bbs_next->bbs_buffer_length;
            mysink->zstd_outBuf.pos = 0;
        }

        // Compress input data to output buffer
        yet_to_flush = ZSTD_compressStream2(mysink->cctx, &mysink->zstd_outBuf,
                                           &inBuf, ZSTD_e_continue);

        if (ZSTD_isError(yet_to_flush))
            elog(ERROR, "could not compress data: %s",
                 ZSTD_getErrorName(yet_to_flush));
    }
}
```