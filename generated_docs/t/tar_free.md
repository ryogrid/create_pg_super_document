# tar_free

## Location
[src/bin/pg_basebackup/walmethods.c:1336-1354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L1336-L1354)

## Overview
The tar_free function is responsible for cleaning up and freeing memory allocated for a tar-based WAL (Write-Ahead Log) method implementation in PostgreSQL's pg_basebackup utility.

## Definition


## Detailed Description
This function serves as a cleanup routine for the tar-based WAL writing method. It performs proper memory deallocation for all resources associated with a TarMethodData structure, including the tar filename, compression-related buffers (when gzip compression is enabled), and the method structure itself. The function ensures no memory leaks occur when destroying a tar WAL method instance.

## Parameters / Member Variables
- : A pointer to the WalWriteMethod structure that needs to be freed. This is cast internally to TarMethodData to access tar-specific fields.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_free](../p/pg_free.md) (for memory deallocation)
  - [TarMethodData](../T/TarMethodData.md) (structure being cleaned up)
  - [WalWriteMethod](../W/WalWriteMethod.md) (base structure type)
  - PG_COMPRESSION_GZIP (compression algorithm constant)
- Called from (representative examples):
  - [CreateWalDirectoryMethod](../C/CreateWalDirectoryMethod.md) (cleanup on error paths)

## Notes and Other Information
- The function is marked as static, indicating it's only used within the walmethods.c file
- Conditional compilation is used for gzip-related cleanup (#ifdef HAVE_LIBZ)
- When gzip compression is enabled, the function specifically frees the zlibOut buffer used for compression output
- This function follows PostgreSQL's standard memory management patterns using pg_free instead of standard free()
- The function assumes the wwmethod parameter is actually a TarMethodData structure, as it performs an unsafe cast