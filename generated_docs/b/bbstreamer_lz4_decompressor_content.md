# bbstreamer_lz4_decompressor_content

## Location
[src/bin/pg_basebackup/bbstreamer_lz4.c:310-389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_lz4.c#L310-L389)

## Overview
Processes LZ4-compressed backup stream data by decompressing input data to an output buffer and forwarding decompressed chunks to the next streamer in the pipeline when the buffer is full.

## Definition
```c
static void bbstreamer_lz4_decompressor_content(bbstreamer *streamer,
                                               bbstreamer_member *member,
                                               const char *data, int len,
                                               bbstreamer_archive_context context)
```

## Detailed Description
This function is the core decompression handler for LZ4-compressed backup streams in pg_basebackup. It uses the LZ4 frame format decompression API to process incoming compressed data. The function operates in a loop, continuously decompressing input data until all available input is consumed. When the output buffer reaches capacity, it forwards the decompressed data to the next streamer in the processing chain and resets the buffer for continued processing.

The function handles dual-parameter behavior of the LZ4F_decompress API, where read_size and out_size parameters serve as both input capacity indicators and return values for actual bytes processed. This allows for efficient streaming decompression without requiring the entire input to be available at once.

## Parameters / Member Variables
- `streamer`: Pointer to the base bbstreamer object, cast to bbstreamer_lz4_frame internally
- `member`: Archive member context information (currently passed through to next streamer)
- `data`: Input buffer containing LZ4-compressed data to be decompressed  
- `len`: Length of the input data buffer in bytes
- `context`: Archive context information indicating the type of data being processed

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_decompress (external LZ4 library function)
  - LZ4F_isError (external LZ4 library function) 
  - LZ4F_getErrorName (external LZ4 library function)
  - pg_log_error (PostgreSQL logging function)
  - [bbstreamer_content](bbstreamer_content.md) (forwards processed data to next streamer)
- Called from (representative examples):
  - Referenced indirectly through bbstreamer function pointer mechanism

## Notes and Other Information
- This is a static function used internally within the LZ4 streaming decompressor implementation
- Error handling logs LZ4 decompression errors but does not abort processing
- The function maintains state across calls through the bbstreamer_lz4_frame structure, particularly the bytes_written field
- Buffer management is handled automatically, with full buffers forwarded downstream and buffer pointers reset for continued processing
- The function is designed to handle partial input data and can be called multiple times to process a complete compressed stream

## Simplified Source

```c
static void
bbstreamer_lz4_decompressor_content(bbstreamer *streamer,
                                  bbstreamer_member *member,
                                  const char *data, int len,
                                  bbstreamer_archive_context context)
{
    bbstreamer_lz4_frame *mystreamer;
    uint8 *input_ptr, *output_ptr;
    size_t input_remaining, output_available;

    mystreamer = (bbstreamer_lz4_frame *) streamer;

    // Set up input and output pointers
    input_ptr = (uint8 *) data;
    output_ptr = (uint8 *) mystreamer->base.bbs_buffer.data + mystreamer->bytes_written;
    input_remaining = len;
    output_available = mystreamer->base.bbs_buffer.maxlen - mystreamer->bytes_written;

    // Process all available input data
    while (input_remaining > 0)
    {
        size_t bytes_read = input_remaining;
        size_t bytes_written = output_available;

        // Decompress data using LZ4 frame format
        size_t result = LZ4F_decompress(mystreamer->dctx,
                                       output_ptr, &bytes_written,
                                       input_ptr, &bytes_read, NULL);

        if (LZ4F_isError(result))
            pg_log_error("could not decompress data: %s", LZ4F_getErrorName(result));

        // Update input position
        input_remaining -= bytes_read;
        input_ptr += bytes_read;
        mystreamer->bytes_written += bytes_written;

        // Forward data when buffer is full
        if (mystreamer->bytes_written >= mystreamer->base.bbs_buffer.maxlen)
        {
            bbstreamer_content(mystreamer->base.bbs_next, member,
                             mystreamer->base.bbs_buffer.data,
                             mystreamer->base.bbs_buffer.maxlen, context);

            // Reset output buffer
            output_available = mystreamer->base.bbs_buffer.maxlen;
            mystreamer->bytes_written = 0;
            output_ptr = (uint8 *) mystreamer->base.bbs_buffer.data;
        }
        else
        {
            // Update output buffer position
            output_available = mystreamer->base.bbs_buffer.maxlen - mystreamer->bytes_written;
            output_ptr += bytes_written;
        }
    }
}
```