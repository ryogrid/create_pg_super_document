# Zstd_write

## Location
[src/bin/pg_dump/compress_zstd.c:354-393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L354-L393)

## Overview
Zstd_write is a static function that handles compression and writing of data to Zstd-compressed files, implementing the core compression logic for pg_dump's compressed stream API.

## Definition
static void Zstd_write(const void *ptr, size_t size, CompressFileHandle *CFH)

## Detailed Description
This function implements the writing mechanism for Zstd-compressed files by compressing input data and writing it to the underlying file. It manages the compression process using Zstd's streaming API, handling lazy initialization of the compression stream on first write, and processing data through input/output buffers. The function ensures all input data is consumed by operating in a loop that compresses data incrementally and writes the compressed output to the file. It uses ZSTD_e_continue mode to indicate that more data may follow, allowing for efficient streaming compression without finalizing the stream.

## Parameters / Member Variables
- : Pointer to the input data buffer to be compressed and written
- : Number of bytes to compress and write from the input buffer
- : Compressed file handle containing Zstd private data, compression parameters, and file pointer

## Dependencies
- Functions called/Symbols referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (struct type)
  - [ZstdCompressorState](ZstdCompressorState.md) (struct type)
  - [pg_malloc0](../p/pg_malloc0.md) (memory allocation)
  - [_ZstdCStreamParams](_ZstdCStreamParams.md) (Zstd stream parameter setup)
  - ZSTD_compressStream2() (Zstd library function)
  - ZSTD_isError() (Zstd library function)
  - ZSTD_getErrorName() (Zstd library function)
  - ZSTD_CStreamOutSize() (Zstd library function)
  - fwrite() (standard library function)
- Called from (representative examples):
  - [InitCompressFileHandleZstd](../I/InitCompressFileHandleZstd.md) (assigned as write function pointer)

## Notes and Other Information
- Implements lazy initialization of the compression stream, creating it only when first write occurs
- Uses ZSTD_e_continue mode for streaming compression, indicating that more data may follow
- Provides comprehensive error handling for both compression failures and file write errors
- Sets errno appropriately for write failures, defaulting to ENOSPC if errno is not set
- Ensures all input data is consumed before returning, maintaining data integrity
- The function integrates with PostgreSQL's error reporting system using pg_fatal()
- Output buffer is allocated based on Zstd's recommended output size for optimal performance

## Simplified Source

```c
static void
Zstd_write(const void *ptr, size_t size, CompressFileHandle *CFH)
{
    ZstdCompressorState *state = (ZstdCompressorState *) CFH->private_data;
    ZSTD_inBuffer *input = &state->input;
    ZSTD_outBuffer *output = &state->output;

    // Setup input buffer with data to compress
    input->src = ptr;
    input->size = size;
    input->pos = 0;

    // Initialize compression stream on first call
    if (state->cstream == NULL) {
        state->output.size = ZSTD_CStreamOutSize();
        state->output.dst = pg_malloc0(state->output.size);
        state->cstream = _ZstdCStreamParams(CFH->compression_spec);
        if (state->cstream == NULL)
            pg_fatal("could not initialize compression library");
    }

    // Compress all input data
    while (input->pos != input->size) {
        output->pos = 0;

        // Compress data chunk
        size_t result = ZSTD_compressStream2(state->cstream, output, input, ZSTD_e_continue);
        if (ZSTD_isError(result))
            pg_fatal("could not write to file: %s", ZSTD_getErrorName(result));

        // Write compressed data to file
        size_t bytes_written = fwrite(output->dst, 1, output->pos, state->fp);
        if (bytes_written != output->pos) {
            errno = (errno) ? errno : ENOSPC;
            pg_fatal("could not write to file: %m");
        }
    }
}
```