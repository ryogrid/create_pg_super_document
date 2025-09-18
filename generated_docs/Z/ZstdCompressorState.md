# ZstdCompressorState

## Location
src/bin/pg_dump/compress_zstd.c: 39 - 51

## Overview
A structure that maintains the state and context for Zstandard (Zstd) compression and decompression operations in PostgreSQL's pg_dump utility.

## Definition
```c
typedef struct ZstdCompressorState
{
    /* This is a normal file to which we read/write compressed data */
    FILE       *fp;

    ZSTD_CStream *cstream;
    ZSTD_DStream *dstream;
    ZSTD_outBuffer output;
    ZSTD_inBuffer input;

    /* pointer to a static string like from strerror(), for Zstd_write() */
    const char *zstderror;
} ZstdCompressorState;
```

## Detailed Description
The `ZstdCompressorState` structure encapsulates all the necessary components for managing Zstandard compression and decompression in pg_dump. It serves as a context holder that maintains both compression and decompression streams along with their associated buffers and file handles. This structure allows pg_dump to efficiently compress database dumps using the Zstd algorithm, providing better compression ratios and performance compared to traditional compression methods.

The structure is designed to handle both compression (for writing dumps) and decompression (for reading compressed dumps), making it versatile for pg_dump and pg_restore operations. It integrates with the Zstandard library's streaming API to provide efficient memory usage and processing of large database dumps.

## Parameters / Member Variables
- `fp`: File pointer to the compressed data file for reading or writing operations
- `cstream`: Zstandard compression stream context used for compressing data
- `dstream`: Zstandard decompression stream context used for decompressing data  
- `output`: Zstandard output buffer structure that holds compressed data being written
- `input`: Zstandard input buffer structure that holds data to be compressed or decompressed
- `zstderror`: Pointer to a static error message string for error reporting in Zstd operations

## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_CStream (from libzstd)
  - ZSTD_DStream (from libzstd)
  - ZSTD_outBuffer (from libzstd)
  - ZSTD_inBuffer (from libzstd)
  - FILE (from standard C library)

- Called from (representative examples):
  - InitCompressorZstd
  - _ZstdWriteCommon
  - EndCompressorZstd
  - WriteDataToArchiveZstd
  - ReadDataFromArchiveZstd
  - Zstd_read_internal
  - Zstd_write
  - Zstd_close
  - Zstd_eof
  - Zstd_open
  - Zstd_get_error

## Notes and Other Information
This structure is specifically designed for pg_dump's Zstandard compression implementation located in `src/bin/pg_dump/compress_zstd.c`. It provides a clean abstraction over the Zstandard library's streaming interface, allowing pg_dump to handle compressed archives efficiently. The structure supports both compression and decompression modes, with the appropriate stream (cstream or dstream) being used depending on the operation being performed. Error handling is facilitated through the zstderror member, which can hold descriptive error messages from the Zstandard library operations.