# ReadDataFromArchiveLZ4

## Location
[src/bin/pg_dump/compress_lz4.c:145-198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L145-L198)

## Overview
Decompresses LZ4-compressed data from a PostgreSQL archive file, reading compressed chunks and writing the decompressed output directly to the archive handle.

## Definition
```c
static void ReadDataFromArchiveLZ4(ArchiveHandle *AH, CompressorState *cs)
```

## Detailed Description
This function implements the decompression logic for LZ4-compressed archive data in pg_dump/pg_restore. It creates an LZ4 decompression context, allocates input and output buffers, and processes the compressed data in chunks. The function reads compressed data using the CompressorState's readF function pointer, decompresses it using LZ4F_decompress(), and writes the decompressed output directly to the archive using ahwrite(). The decompression is performed incrementally, processing data as it becomes available rather than loading the entire archive into memory.

## Parameters / Member Variables
- `AH`: Pointer to the ArchiveHandle structure representing the archive being processed
- `cs`: Pointer to the CompressorState structure containing compression/decompression state and function pointers

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_createDecompressionContext
  - LZ4F_decompress
  - LZ4F_freeDecompressionContext
  - LZ4F_isError
  - LZ4F_getErrorName
  - [pg_malloc0](../p/pg_malloc0.md)
  - [pg_free](../p/pg_free.md)
  - [ahwrite](../a/ahwrite.md)
  - [pg_fatal](../p/pg_fatal.md)
- Constants used:
  - DEFAULT_IO_BUFFER_SIZE
  - LZ4F_VERSION
- Called from (representative examples):
  - No direct references found (likely used via function pointer)

## Notes and Other Information
- This is a static function internal to the compress_lz4.c module
- Uses DEFAULT_IO_BUFFER_SIZE for both input and output buffer allocation
- Performs error handling with pg_fatal() for all LZ4 library errors
- Processes data incrementally in chunks to minimize memory usage
- Automatically manages LZ4 decompression context lifecycle
- Part of PostgreSQL's pg_dump LZ4 decompression implementation
- The function is designed to work with the archive format's streaming interface
- Uses memset() to clear output buffer before each decompression operation

## Simplified Source

```c
static void
ReadDataFromArchiveLZ4(ArchiveHandle *AH, CompressorState *cs)
{
    size_t r, readbuflen;
    char *outbuf, *readbuf;
    LZ4F_decompressionContext_t ctx = NULL;
    LZ4F_decompressOptions_t dec_opt;
    LZ4F_errorCode_t status;

    // Initialize LZ4 decompression context
    memset(&dec_opt, 0, sizeof(dec_opt));
    status = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
    if (LZ4F_isError(status))
        pg_fatal("could not create LZ4 decompression context: %s",
                 LZ4F_getErrorName(status));

    // Allocate buffers for input and output
    outbuf = pg_malloc0(DEFAULT_IO_BUFFER_SIZE);
    readbuf = pg_malloc0(DEFAULT_IO_BUFFER_SIZE);
    readbuflen = DEFAULT_IO_BUFFER_SIZE;

    // Read and decompress data in chunks
    while ((r = cs->readF(AH, &readbuf, &readbuflen)) > 0)
    {
        char *readp = readbuf;
        char *readend = readbuf + r;

        // Process each chunk of compressed data
        while (readp < readend)
        {
            size_t out_size = DEFAULT_IO_BUFFER_SIZE;
            size_t read_size = readend - readp;

            // Decompress current chunk
            memset(outbuf, 0, DEFAULT_IO_BUFFER_SIZE);
            status = LZ4F_decompress(ctx, outbuf, &out_size,
                                     readp, &read_size, &dec_opt);
            if (LZ4F_isError(status))
                pg_fatal("could not decompress: %s", LZ4F_getErrorName(status));

            // Write decompressed data to archive
            ahwrite(outbuf, 1, out_size, AH);
            readp += read_size;
        }
    }

    // Cleanup
    pg_free(outbuf);
    pg_free(readbuf);

    status = LZ4F_freeDecompressionContext(ctx);
    if (LZ4F_isError(status))
        pg_fatal("could not free LZ4 decompression context: %s",
                 LZ4F_getErrorName(status));
}
```