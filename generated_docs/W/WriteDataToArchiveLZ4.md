# WriteDataToArchiveLZ4

## Location
[src/bin/pg_dump/compress_lz4.c:199-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L199-L237)

## Overview
Compresses input data using LZ4 compression and writes the compressed output to a PostgreSQL archive file, handling header flushing and chunked data processing.

## Definition
```c
static void WriteDataToArchiveLZ4(ArchiveHandle *AH, CompressorState *cs, const void *data, size_t dLen)
```

## Detailed Description
This function implements the compression logic for writing data to LZ4-compressed archive files in pg_dump. It handles the initial header flush if needed, then processes the input data in chunks using LZ4F_compressUpdate(). The function breaks large data blocks into DEFAULT_IO_BUFFER_SIZE chunks to manage memory usage efficiently. Each compressed chunk is immediately written to the archive using the CompressorState's writeF function pointer. The function maintains the LZ4State compression context throughout the operation and provides error handling for compression failures.

## Parameters / Member Variables
- `AH`: Pointer to the ArchiveHandle structure representing the archive being written to
- `cs`: Pointer to the CompressorState structure containing compression state and function pointers
- `data`: Pointer to the input data to be compressed
- `dLen`: Size in bytes of the input data to compress

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_compressUpdate
  - LZ4F_isError
  - LZ4F_getErrorName
  - [pg_fatal](../p/pg_fatal.md)
- Constants used:
  - DEFAULT_IO_BUFFER_SIZE
- Types used:
  - [LZ4State](../L/LZ4State.md)
  - [CompressorState](../C/CompressorState.md)
- Called from (representative examples):
  - No direct references found (likely used via function pointer)

## Notes and Other Information
- This is a static function internal to the compress_lz4.c module
- Handles header flushing on first call via the needs_header_flush flag
- Processes data in chunks to prevent excessive memory usage
- Uses LZ4F_compressUpdate for incremental compression
- Provides comprehensive error handling with pg_fatal() for compression failures
- Part of PostgreSQL's pg_dump LZ4 compression implementation
- The function advances the data pointer as it processes chunks
- Immediately writes compressed data to avoid buffering large amounts of compressed data
- Works with the streaming compression context maintained in the LZ4State structure

## Simplified Source

```c
static void
WriteDataToArchiveLZ4(ArchiveHandle *AH, CompressorState *cs,
                      const void *data, size_t dLen)
{
    LZ4State *state = (LZ4State *) cs->private_data;
    size_t remaining = dLen;
    size_t status, chunk;

    // Write header if this is the first call
    if (state->needs_header_flush)
    {
        cs->writeF(AH, state->buffer, state->compressedlen);
        state->needs_header_flush = false;
    }

    // Process data in chunks
    while (remaining > 0)
    {
        // Determine chunk size (max DEFAULT_IO_BUFFER_SIZE)
        chunk = (remaining > DEFAULT_IO_BUFFER_SIZE) ?
                DEFAULT_IO_BUFFER_SIZE : remaining;
        remaining -= chunk;

        // Compress current chunk
        status = LZ4F_compressUpdate(state->ctx,
                                     state->buffer, state->buflen,
                                     data, chunk, NULL);
        if (LZ4F_isError(status))
            pg_fatal("could not compress data: %s", LZ4F_getErrorName(status));

        // Write compressed data to archive
        cs->writeF(AH, state->buffer, status);

        // Advance to next chunk
        data = ((char *) data) + chunk;
    }
}
```