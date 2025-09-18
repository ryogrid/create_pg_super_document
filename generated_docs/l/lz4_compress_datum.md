# lz4_compress_datum

## Location
[src/backend/access/common/toast_compression.c:139-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_compression.c#L139-L181)

## Overview
Compresses a varlena data structure using the LZ4 compression algorithm, which is an alternative to PGLZ for PostgreSQL's TOAST system, offering faster compression and decompression speeds.

## Definition


## Detailed Description
This function implements LZ4 compression for PostgreSQL's TOAST system when LZ4 support is available at compile time. It takes a varlena structure and attempts to compress it using the LZ4 algorithm. The function first checks if LZ4 support is compiled in; if not, it calls NO_LZ4_SUPPORT() and returns NULL. When LZ4 is available, it calculates the maximum possible compressed size, allocates memory accordingly, and performs the compression. If compression results in a larger size than the original (indicating incompressible data), it frees the memory and returns NULL to signal that compression should not be used.

The function handles error conditions by checking the LZ4 compression result and reporting errors if compression completely fails.

## Parameters / Member Variables
- : A pointer to the input varlena structure containing the data to be compressed

## Dependencies
- Functions called/Symbols referenced:
  - NO_LZ4_SUPPORT (macro to handle missing LZ4 support)
  - VARSIZE_ANY_EXHDR (macro to get data size excluding header)
  - LZ4_compressBound (LZ4 function to calculate max compressed size)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - VARHDRSZ_COMPRESSED (compressed varlena header size)
  - LZ4_compress_default (core LZ4 compression function)
  - VARDATA_ANY (macro to get data portion of varlena)
  - elog (PostgreSQL logging/error function)
  - ERROR (error level constant)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - SET_VARSIZE_COMPRESSED (macro to set compressed size header)
- Called from (representative examples):
  - [toast_compress_datum](../t/toast_compress_datum.md) (in src/backend/access/common/toast_internals.c:71)
  - Referenced in CompressionMethodIsValid (in src/include/access/toast_compression.h:63)

## Notes and Other Information
- Only available when PostgreSQL is compiled with LZ4 support (USE_LZ4 defined)
- Returns NULL if LZ4 support is not available at compile time
- Returns NULL if the compressed data would be larger than the original (incompressible data)
- Raises an ERROR if LZ4 compression fails completely
- LZ4 generally offers faster compression/decompression compared to PGLZ but may have different compression ratios
- Part of PostgreSQL's TOAST compression infrastructure, providing an alternative to PGLZ
- Located in src/backend/access/common/toast_compression.c:139-181