# ReadDataFromArchiveGzip

## Location
[src/bin/pg_dump/compress_gzip.c:163-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L163-L229)

## Overview
Reads and decompresses gzip-compressed data from a PostgreSQL archive using zlib's inflate functionality.

## Definition

```c
static void
ReadDataFromArchiveGzip(ArchiveHandle *AH, CompressorState *cs)
```
## Detailed Description
This function handles the decompression of gzip-compressed data during the reading phase of PostgreSQL's pg_dump/pg_restore operations. It initializes a zlib decompression stream, reads compressed data in chunks through the CompressorState's readF function, and decompresses the data using zlib's inflate() function. The decompressed data is then written to the archive using ahwrite(). The function includes proper error handling for decompression failures and ensures complete decompression by continuing to call inflate() until Z_STREAM_END is reached.

## Parameters / Member Variables
- `*AH`: Archive handle containing the archive state and operations
- `*cs`: Compressor state containing the read function and buffer management
## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md)
  - inflateInit
  - inflate
  - inflateEnd
  - [ahwrite](../a/ahwrite.md)
  - [pg_fatal](../p/pg_fatal.md)
  - DEFAULT_IO_BUFFER_SIZE
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Uses DEFAULT_IO_BUFFER_SIZE for both input and output buffers
- Implements a two-phase decompression: first reading available input data, then flushing any remaining compressed data
- Properly handles zlib error codes and provides detailed error messages
- Memory management includes allocation and deallocation of zlib stream structure and buffers
- Null-terminates output buffer for safety before writing to archive

## Simplified Source

```c
static void
ReadDataFromArchiveGzip(ArchiveHandle *AH, CompressorState *cs)
{
    // Initialize zlib decompression stream
    z_streamp zp = pg_malloc(sizeof(z_stream));
    zp->zalloc = Z_NULL;
    zp->zfree = Z_NULL;
    zp->opaque = Z_NULL;

    // Allocate input and output buffers
    size_t buflen = DEFAULT_IO_BUFFER_SIZE;
    char *buf = pg_malloc(buflen);
    char *out = pg_malloc(DEFAULT_IO_BUFFER_SIZE + 1);

    if (inflateInit(zp) != Z_OK) {
        pg_fatal("could not initialize compression library: %s", zp->msg);
    }

    // Phase 1: Read and decompress input data
    size_t cnt;
    while ((cnt = cs->readF(AH, &buf, &buflen))) {
        zp->next_in = (void *) buf;
        zp->avail_in = cnt;

        // Decompress all available input
        while (zp->avail_in > 0) {
            zp->next_out = (void *) out;
            zp->avail_out = DEFAULT_IO_BUFFER_SIZE;

            int res = inflate(zp, 0);
            if (res != Z_OK && res != Z_STREAM_END) {
                pg_fatal("could not uncompress data: %s", zp->msg);
            }

            // Write decompressed data to archive
            size_t bytes_out = DEFAULT_IO_BUFFER_SIZE - zp->avail_out;
            out[bytes_out] = '\0';
            ahwrite(out, 1, bytes_out, AH);
        }
    }

    // Phase 2: Flush remaining compressed data
    zp->next_in = NULL;
    zp->avail_in = 0;
    int res = Z_OK;
    while (res != Z_STREAM_END) {
        zp->next_out = (void *) out;
        zp->avail_out = DEFAULT_IO_BUFFER_SIZE;
        res = inflate(zp, 0);
        if (res != Z_OK && res != Z_STREAM_END) {
            pg_fatal("could not uncompress data: %s", zp->msg);
        }

        size_t bytes_out = DEFAULT_IO_BUFFER_SIZE - zp->avail_out;
        out[bytes_out] = '\0';
        ahwrite(out, 1, bytes_out, AH);
    }

    // Cleanup
    if (inflateEnd(zp) != Z_OK) {
        pg_fatal("could not close compression library: %s", zp->msg);
    }
    free(buf);
    free(out);
    free(zp);
}
```