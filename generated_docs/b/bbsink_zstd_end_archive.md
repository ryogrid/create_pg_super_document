# bbsink_zstd_end_archive

## Location
[src/backend/backup/basebackup_zstd.c:235-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_zstd.c#L235-L281)

## Overview
Finalizes zstd compression by flushing internal buffers, ending the compression frame, and forwarding any remaining compressed data to the next sink in the chain.

## Definition

```c
static void
bbsink_zstd_end_archive(bbsink *sink)
```
## Detailed Description
This function completes the zstd compression for an archive by flushing any remaining data from zstd's internal buffers and properly ending the compression frame. It uses ZSTD_e_end mode to signal the end of compression, which causes zstd to flush all buffered data and write frame termination markers. The function continues compressing until no more data needs to be flushed, handles output buffer management by sending data to the next sink when space is needed, and ensures any final compressed bytes are forwarded before notifying the next sink that the archive has ended.

## Parameters / Member Variables
- `*sink`: Pointer to the bbsink structure (cast to bbsink_zstd internally) that contains compression context and buffers
## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_compressBound (calculates space needed for final compression)
  - ZSTD_compressStream2 (performs final compression with ZSTD_e_end mode)
  - ZSTD_isError (checks for compression errors)
  - ZSTD_getErrorName (gets error description)  
  - [bbsink_archive_contents](bbsink_archive_contents.md) (sends compressed data to next sink)
  - [bbsink_forward_end_archive](bbsink_forward_end_archive.md) (notifies next sink that archive ended)
  - elog (error logging)
- Called from (representative examples):
  - Through bbsink_zstd_ops function pointer table

## Notes and Other Information
- Uses ZSTD_e_end mode to signal compression completion and flush internal buffers
- Loops until yet_to_flush returns 0, indicating all buffered data has been output
- Uses empty input buffer (NULL, 0, 0) since no new data is being compressed
- Manages output buffer space by flushing to next sink when needed
- Ensures any remaining bytes in output buffer are sent to next sink before ending
- Calls bbsink_forward_end_archive to properly terminate the archive in the sink chain
- Function is static and called through the bbsink operations table
- Critical for proper zstd frame termination and ensuring no compressed data is lost

## Simplified Source

```c
static void bbsink_zstd_end_archive(bbsink *sink) {
    bbsink_zstd *mysink = (bbsink_zstd *) sink;
    size_t yet_to_flush;

    // Flush zstd internal buffers and end compression frame
    do {
        ZSTD_inBuffer in = {NULL, 0, 0}; // Empty input - just flushing
        size_t max_needed = ZSTD_compressBound(0);

        // Check if output buffer has enough space
        if (mysink->zstd_outBuf.size - mysink->zstd_outBuf.pos < max_needed) {
            // Flush current data to next sink
            bbsink_archive_contents(mysink->base.bbs_next,
                                   mysink->zstd_outBuf.pos);

            // Reset output buffer
            mysink->zstd_outBuf.dst = mysink->base.bbs_next->bbs_buffer;
            mysink->zstd_outBuf.size = mysink->base.bbs_next->bbs_buffer_length;
            mysink->zstd_outBuf.pos = 0;
        }

        // End compression and flush remaining data
        yet_to_flush = ZSTD_compressStream2(mysink->cctx,
                                           &mysink->zstd_outBuf,
                                           &in, ZSTD_e_end);

        if (ZSTD_isError(yet_to_flush))
            elog(ERROR, "could not compress data: %s",
                 ZSTD_getErrorName(yet_to_flush));

    } while (yet_to_flush > 0); // Continue until all data is flushed

    // Send any remaining compressed bytes to next sink
    if (mysink->zstd_outBuf.pos > 0)
        bbsink_archive_contents(mysink->base.bbs_next,
                               mysink->zstd_outBuf.pos);

    // Notify next sink that archive has ended
    bbsink_forward_end_archive(sink);
}
```