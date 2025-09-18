# lz4_decompress_datum

## Location
[src/backend/access/common/toast_compression.c:182-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_compression.c#L182-L214)

## Overview
Decompresses a varlena data structure that was previously compressed using the LZ4 compression algorithm, restoring the original uncompressed data with fast decompression performance.

## Definition


## Detailed Description
This function performs LZ4 decompression for PostgreSQL's TOAST system when LZ4 support is available at compile time. It takes a compressed varlena structure and decompresses it back to its original form using the LZ4 algorithm. The function first checks if LZ4 support is compiled in; if not, it calls NO_LZ4_SUPPORT() and returns NULL. When LZ4 is available, it allocates memory for the decompressed data based on the stored original size, then calls LZ4_decompress_safe to safely decompress the data with bounds checking. If decompression fails, it raises an error indicating data corruption.

The function uses LZ4's safe decompression function which includes bounds checking to prevent buffer overflows, making it secure against corrupted or malicious compressed data.

## Parameters / Member Variables
- : A pointer to the LZ4-compressed varlena structure to be decompressed

## Dependencies
- Functions called/Symbols referenced:
  - NO_LZ4_SUPPORT (macro to handle missing LZ4 support)
  - VARDATA_COMPRESSED_GET_EXTSIZE (macro to get original uncompressed size)
  - VARHDRSZ (standard varlena header size)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - LZ4_decompress_safe (secure LZ4 decompression function)
  - VARHDRSZ_COMPRESSED (compressed varlena header size)
  - VARDATA (macro to get data portion of varlena)
  - VARSIZE (macro to get varlena total size)
  - ereport (PostgreSQL error reporting)
  - ERROR (error level constant)
  - [errcode](../e/errcode.md) (error code function)
  - ERRCODE_DATA_CORRUPTED (specific error code for data corruption)
  - [errmsg_internal](../e/errmsg_internal.md) (internal error message function)
  - SET_VARSIZE (macro to set varlena size header)
- Called from (representative examples):
  - [toast_decompress_datum](../t/toast_decompress_datum.md) (in src/backend/access/common/detoast.c:487)
  - [lz4_decompress_datum_slice](lz4_decompress_datum_slice.md) (in src/backend/access/common/toast_compression.c:226)
  - Referenced in CompressionMethodIsValid (in src/include/access/toast_compression.h:64)

## Notes and Other Information
- Only available when PostgreSQL is compiled with LZ4 support (USE_LZ4 defined)
- Returns NULL if LZ4 support is not available at compile time
- Uses LZ4_decompress_safe for secure decompression with bounds checking
- Throws an ERROR with ERRCODE_DATA_CORRUPTED if decompression fails
- LZ4 generally offers faster decompression performance compared to PGLZ
- Memory allocation is based on the original uncompressed size stored in the compressed data header
- Part of PostgreSQL's TOAST decompression infrastructure, providing an alternative to PGLZ
- Located in src/backend/access/common/toast_compression.c:182-214