# pglz_decompress_datum

## Location
[src/backend/access/common/toast_compression.c:82-108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_compression.c#L82-L108)

## Overview
Decompresses a varlena data structure that was previously compressed using the PGLZ compression algorithm, restoring the original uncompressed data.

## Definition


## Detailed Description
This function performs PGLZ decompression for PostgreSQL's TOAST system. It takes a compressed varlena structure and decompresses it back to its original form. The function first allocates memory for the decompressed data based on the stored original size, then calls the core PGLZ decompression routine. If decompression fails (indicating corrupted data), it raises an error. Upon successful decompression, it properly sets the varlena size header and returns the decompressed data.

The function handles error conditions by checking the decompression result and reporting data corruption errors when decompression fails.

## Parameters / Member Variables
- : A pointer to the compressed varlena structure to be decompressed

## Dependencies
- Functions called/Symbols referenced:
  - VARDATA_COMPRESSED_GET_EXTSIZE (macro to get original uncompressed size)
  - VARHDRSZ (standard varlena header size)
  - [palloc](palloc.md) (PostgreSQL memory allocation)
  - pglz_decompress (core PGLZ decompression function)
  - VARHDRSZ_COMPRESSED (compressed varlena header size)
  - VARSIZE (macro to get varlena total size)
  - VARDATA (macro to get data portion of varlena)
  - ereport (PostgreSQL error reporting)
  - ERROR (error level constant)
  - [errcode](../e/errcode.md) (error code function)
  - ERRCODE_DATA_CORRUPTED (specific error code for data corruption)
  - [errmsg_internal](../e/errmsg_internal.md) (internal error message function)
  - SET_VARSIZE (macro to set varlena size header)
- Called from (representative examples):
  - [toast_decompress_datum](../t/toast_decompress_datum.md) (in src/backend/access/common/detoast.c:485)
  - Referenced in CompressionMethodIsValid (in src/include/access/toast_compression.h:58)

## Notes and Other Information
- Throws an ERROR with ERRCODE_DATA_CORRUPTED if decompression fails, indicating corrupted compressed data
- Memory allocation is based on the original uncompressed size stored in the compressed data header
- Part of PostgreSQL's TOAST decompression infrastructure, specifically handling PGLZ method
- The function assumes the input data is valid PGLZ-compressed data
- Located in src/backend/access/common/toast_compression.c:82-108