# bbstreamer_gzip_decompressor_content

## Location
[src/bin/pg_basebackup/bbstreamer_gzip.c:261-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_gzip.c#L261-L315)

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
  - [bbstreamer](bbstreamer.md) (base streamer type)
  - bbstreamer_member (member information structure)
  - [bbstreamer_archive_context](bbstreamer_archive_context.md) (context information type)
  - [bbstreamer_gzip_decompressor](bbstreamer_gzip_decompressor.md) (internal decompressor structure)
  - [bbstreamer_content](bbstreamer_content.md) (function to forward data to next streamer)
- Called from (representative examples):
  - No direct references found (likely called through function pointer in ops structure)

## Notes and Other Information
- This is a static function, only accessible within the bbstreamer_gzip.c compilation unit
- Uses zlib's inflate() function with Z_NO_FLUSH flag for incremental decompression
- Implements a buffering strategy that forwards data when the output buffer is full
- Handles z_stream state updates for both input consumption and output generation
- Error handling includes checking for Z_STREAM_ERROR and logging decompression failures
- Part of the callback-based streaming architecture where this function is invoked through the ops structure

## Simplified Source

```c
static void
bbstreamer_gzip_decompressor_content(bbstreamer *streamer,
                                   bbstreamer_member *member,
                                   const char *data, int len,
                                   bbstreamer_archive_context context)
{
    bbstreamer_gzip_decompressor *mystreamer;
    z_stream *zs;

    mystreamer = (bbstreamer_gzip_decompressor *) streamer;
    zs = &mystreamer->zstream;

    // Set up input data for decompression
    zs->next_in = (const uint8 *) data;
    zs->avail_in = len;

    // Process all available input data
    while (zs->avail_in > 0)
    {
        // Configure output buffer
        zs->next_out = (uint8 *) mystreamer->base.bbs_buffer.data + mystreamer->bytes_written;
        zs->avail_out = mystreamer->base.bbs_buffer.maxlen - mystreamer->bytes_written;

        // Decompress data using zlib
        int result = inflate(zs, Z_NO_FLUSH);

        if (result == Z_STREAM_ERROR)
            pg_log_error("could not decompress data: %s", zs->msg);

        // Update bytes written based on decompression output
        mystreamer->bytes_written = mystreamer->base.bbs_buffer.maxlen - zs->avail_out;

        // Forward data when buffer is full
        if (mystreamer->bytes_written >= mystreamer->base.bbs_buffer.maxlen)
        {
            bbstreamer_content(mystreamer->base.bbs_next, member,
                             mystreamer->base.bbs_buffer.data,
                             mystreamer->base.bbs_buffer.maxlen, context);
            mystreamer->bytes_written = 0;
        }
    }
}
```