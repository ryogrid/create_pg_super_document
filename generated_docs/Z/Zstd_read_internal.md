# Zstd_read_internal

## Location
[src/bin/pg_dump/compress_zstd.c:262-353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L262-L353)

## Overview
Zstd_read_internal is a static function that provides the core streaming decompression functionality for Zstd-compressed files, implementing buffered reading with automatic decompression and error handling capabilities.

## Definition
static size_t Zstd_read_internal(void *ptr, size_t size, CompressFileHandle *CFH, bool exit_on_error)

## Detailed Description
This function implements the internal reading mechanism for Zstd-compressed files in pg_dump's compressed stream API. It manages the decompression process by maintaining input and output buffers, reading compressed data from the file as needed, and decompressing it using Zstd's streaming API. The function handles lazy initialization of the decompression stream on first call, manages buffer states efficiently, and provides flexible error handling based on the exit_on_error parameter. It operates in a loop to fill the requested output buffer, reading and decompressing data incrementally until the requested size is satisfied or end-of-file is reached.

## Parameters / Member Variables
- : Pointer to the output buffer where decompressed data will be stored
- : Number of bytes to read and decompress into the output buffer
- : Compressed file handle containing the Zstd private data and file pointer
- : Boolean flag controlling whether to exit fatally on errors or return -1

## Dependencies
- Functions called/Symbols referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (struct type)
  - [ZstdCompressorState](ZstdCompressorState.md) (struct type)
  - [pg_malloc0](../p/pg_malloc0.md) (memory allocation)
  - unconstify (utility function)
  - ZSTD_createDStream() (Zstd library function)
  - ZSTD_decompressStream() (Zstd library function)
  - ZSTD_isError() (Zstd library function)
  - ZSTD_getErrorName() (Zstd library function)
  - ZSTD_DStreamInSize() (Zstd library function)
  - fread() (standard library function)
  - ferror() (standard library function)
- Called from (representative examples):
  - [Zstd_gets](Zstd_gets.md) (wrapper function)
  - [Zstd_read](Zstd_read.md) (wrapper function)

## Notes and Other Information
- Implements lazy initialization of the decompression stream, allowing the function to be called before full initialization
- Uses efficient buffer management with input consumption tracking and automatic buffer refilling
- Provides dual error handling modes: fatal exit for critical operations or return -1 for recoverable errors
- Handles end-of-frame detection (res == 0) and end-of-file conditions gracefully
- Uses assertions to validate buffer state consistency throughout the decompression process
- The function is designed to work with standard file I/O operations and integrates with PostgreSQL's memory management

## Simplified Source

```c
static size_t
Zstd_read_internal(void *ptr, size_t size, CompressFileHandle *CFH, bool exit_on_error)
{
    ZstdCompressorState *state = (ZstdCompressorState *) CFH->private_data;
    ZSTD_inBuffer *input = &state->input;
    ZSTD_outBuffer *output = &state->output;

    // Initialize decompression stream on first call
    if (state->dstream == NULL) {
        state->input.src = pg_malloc0(ZSTD_DStreamInSize());
        state->dstream = ZSTD_createDStream();
        if (state->dstream == NULL) {
            if (exit_on_error)
                pg_fatal("could not initialize compression library");
            return -1;
        }
    }

    // Setup output buffer
    output->size = size;
    output->dst = ptr;
    output->pos = 0;

    // Main decompression loop
    for (;;) {
        // Reset input buffer when consumed
        if (input->pos == input->size) {
            input->size = 0;
            input->pos = 0;
        }

        // Read more compressed data if needed
        if (input->pos == input->size) {
            size_t bytes_read = fread(unconstify(void *, input->src), 1,
                                     ZSTD_DStreamInSize(), state->fp);
            if (ferror(state->fp)) {
                if (exit_on_error)
                    pg_fatal("could not read from input file: %m");
                return -1;
            }
            input->size = bytes_read;
            if (bytes_read == 0)
                break; // End of file
        }

        // Decompress available data
        while (input->pos < input->size) {
            size_t result = ZSTD_decompressStream(state->dstream, output, input);
            if (ZSTD_isError(result)) {
                if (exit_on_error)
                    pg_fatal("could not decompress data: %s", ZSTD_getErrorName(result));
                return -1;
            }

            // Check if output buffer is full or frame is complete
            if (output->pos == output->size || result == 0)
                break;
        }

        if (output->pos == output->size)
            break; // Output buffer is full
    }

    return output->pos;
}
```