# pglz_decompress_datum_slice

## Location
[src/backend/access/common/toast_compression.c:109-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_compression.c#L109-L138)

## Overview
Decompresses a partial portion (slice) of a varlena data structure that was compressed using PGLZ, allowing for efficient retrieval of only a subset of the original data.

## Definition

```c
struct varlena *
pglz_decompress_datum_slice(const struct varlena *value,
							int32 slicelength)
```
## Detailed Description
This function performs partial PGLZ decompression for PostgreSQL's TOAST system, allowing extraction of only a portion of the original uncompressed data without decompressing the entire datum. This is particularly useful for large TOAST values where only a small portion of the data is needed, providing significant performance benefits. The function allocates memory only for the requested slice size and calls the core PGLZ decompression routine with partial decompression enabled (false flag). If decompression fails, it reports data corruption errors.

Unlike the full decompression function, this variant allows specifying the exact amount of data to decompress, making it efficient for substring operations and partial data access patterns.

## Parameters / Member Variables
- : A pointer to the compressed varlena structure to be partially decompressed
- : The number of bytes to decompress from the beginning of the original uncompressed data

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](palloc.md) (PostgreSQL memory allocation)
  - VARHDRSZ (standard varlena header size)
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
  - [toast_decompress_datum_slice](../t/toast_decompress_datum_slice.md) (in src/backend/access/common/detoast.c:528)
  - Referenced in CompressionMethodIsValid (in src/include/access/toast_compression.h:59)

## Notes and Other Information
- Optimized for partial data access, avoiding the overhead of decompressing entire large values
- Memory allocation is based on the requested slice length rather than the full original size
- Passes false to pglz_decompress to enable partial decompression mode
- Throws an ERROR with ERRCODE_DATA_CORRUPTED if decompression fails
- Part of PostgreSQL's efficient TOAST access infrastructure for large values
- Particularly useful for substring operations on compressed text data
- Located in src/backend/access/common/toast_compression.c:109-138