# CreateWalTarMethod

## Location
[src/bin/pg_basebackup/walmethods.c:1355-1382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L1355-L1382)

## Overview
CreateWalTarMethod creates and initializes a tar-based WAL (Write-Ahead Log) writing method for PostgreSQL's pg_basebackup utility, supporting both uncompressed tar and gzip-compressed tar formats.

## Definition

```c
WalWriteMethod *
CreateWalTarMethod(const char *tarbase,
				   pg_compress_algorithm compression_algorithm,
				   int compression_level, bool sync)
```
## Detailed Description
This function creates a tar-based WAL method implementation that packages WAL files into tar archives. It allocates and initializes a TarMethodData structure, sets up the appropriate file operations through WalTarMethodOps, and configures compression settings. The function supports both uncompressed (.tar) and gzip-compressed (.tar.gz) output formats. Currently, only zlib/gzip compression is supported, though the compression_algorithm parameter exists for future extensibility and symmetry with CreateWalDirectoryMethod.

## Parameters / Member Variables
- `*tarbase`: Base filename for the tar archive (without extension)
- `compression_algorithm`: Compression algorithm to use (currently only PG_COMPRESSION_GZIP is meaningful)
- `compression_level`: Compression level for gzip compression (ignored for uncompressed tar)
- `sync`: Whether to perform fsync operations for data integrity
## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](../p/pg_malloc0.md) (for memory allocation)
  - clear_error (to initialize error state)
  - sprintf (for filename construction)
  - [pg_malloc](../p/pg_malloc.md) (for compression buffer allocation)
  - WalTarMethodOps (function pointer table for tar operations)
  - [TarMethodData](../T/TarMethodData.md) (internal data structure)
  - PG_COMPRESSION_GZIP (compression algorithm constant)
  - ZLIB_OUT_SIZE (compression buffer size constant)
- Called from (representative examples):
  - [LogStreamerMain](../L/LogStreamerMain.md) (in pg_basebackup.c for WAL streaming)

## Notes and Other Information
- The compression_algorithm parameter is currently ignored except for distinguishing gzip vs uncompressed output
- Only zlib/gzip compression is supported in the tar method family
- The function automatically appends appropriate file extensions (.tar or .tar.gz)
- When gzip compression is enabled, a compression output buffer of ZLIB_OUT_SIZE + 1 bytes is allocated
- The returned WalWriteMethod pointer should be freed using the tar_free function through the ops table
- The function follows PostgreSQL's zero-initialization pattern using pg_malloc0 for the main structure
- File descriptor is initialized to -1 to indicate no open file initially

## Simplified Source

```c
WalWriteMethod *CreateWalTarMethod(const char *tarbase,
                                   pg_compress_algorithm compression_algorithm,
                                   int compression_level, bool sync) {
    TarMethodData *wwmethod;

    // Determine file extension based on compression
    const char *suffix = (compression_algorithm == PG_COMPRESSION_GZIP) ?
                        ".tar.gz" : ".tar";

    // Allocate and initialize the TAR method structure
    wwmethod = pg_malloc0(sizeof(TarMethodData));

    // Set up function pointer table and basic configuration
    *((const WalWriteMethodOps **) &wwmethod->base.ops) = &WalTarMethodOps;
    wwmethod->base.compression_algorithm = compression_algorithm;
    wwmethod->base.compression_level = compression_level;
    wwmethod->base.sync = sync;
    clear_error(&wwmethod->base);

    // Build the tar filename with appropriate extension
    wwmethod->tarfilename = pg_malloc0(strlen(tarbase) + strlen(suffix) + 1);
    sprintf(wwmethod->tarfilename, "%s%s", tarbase, suffix);

    // Initialize file descriptor
    wwmethod->fd = -1;

    // Allocate compression buffer if using gzip
    if (compression_algorithm == PG_COMPRESSION_GZIP)
        wwmethod->zlibOut = (char *) pg_malloc(ZLIB_OUT_SIZE + 1);

    return &wwmethod->base;
}
```