# CreateWalTarMethod

## Location
[src/bin/pg_basebackup/walmethods.c:1355-1382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L1355-L1382)

## Overview
CreateWalTarMethod creates and initializes a tar-based WAL (Write-Ahead Log) writing method for PostgreSQL's pg_basebackup utility, supporting both uncompressed tar and gzip-compressed tar formats.

## Definition


## Detailed Description
This function creates a tar-based WAL method implementation that packages WAL files into tar archives. It allocates and initializes a TarMethodData structure, sets up the appropriate file operations through WalTarMethodOps, and configures compression settings. The function supports both uncompressed (.tar) and gzip-compressed (.tar.gz) output formats. Currently, only zlib/gzip compression is supported, though the compression_algorithm parameter exists for future extensibility and symmetry with CreateWalDirectoryMethod.

## Parameters / Member Variables
- : Base filename for the tar archive (without extension)
- : Compression algorithm to use (currently only PG_COMPRESSION_GZIP is meaningful)
- : Compression level for gzip compression (ignored for uncompressed tar)
- : Whether to perform fsync operations for data integrity

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0 (for memory allocation)
  - clear_error (to initialize error state)
  - sprintf (for filename construction)
  - pg_malloc (for compression buffer allocation)
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