# lz4_decompress_datum_slice

## Location
[src/backend/access/common/toast_compression.c:215-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_compression.c#L215-L253)

## Overview
Partially decompresses a varlena data structure that was previously compressed using the LZ4 compression algorithm, returning only a specified slice (prefix) of the original uncompressed data.

## Definition
struct varlena *lz4_decompress_datum_slice(const struct varlena *value, int32 slicelength)

## Detailed Description
This function provides partial decompression functionality for LZ4-compressed varlena data, allowing PostgreSQL to extract only the beginning portion of compressed data without fully decompressing the entire datum. This optimization is particularly valuable for operations that only need to examine the first part of large compressed values.

The function performs version checking to ensure compatibility with LZ4 library features. If the installed LZ4 version is older than 1.8.3, it falls back to full decompression using  since partial decompression is not supported in earlier versions.

The implementation uses LZ4's  function to perform the actual decompression, which stops after producing the requested number of bytes rather than decompressing the entire compressed data. This provides both memory and CPU efficiency benefits when only a portion of the data is needed.

## Parameters / Member Variables
- : Pointer to the compressed varlena structure containing LZ4-compressed data
- : Number of bytes to decompress from the beginning of the original uncompressed data

## Dependencies
- Functions called/Symbols referenced:
  - NO_LZ4_SUPPORT (macro for error handling when LZ4 support is unavailable)
  - [lz4_decompress_datum](lz4_decompress_datum.md) (fallback for older LZ4 versions)
  - LZ4_versionNumber (LZ4 library version check)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - LZ4_decompress_safe_partial (LZ4 library partial decompression)
  - ereport (PostgreSQL error reporting)
  - SET_VARSIZE (macro to set varlena size)
  - VARHDRSZ_COMPRESSED, VARDATA, VARSIZE (varlena manipulation macros)
  - ERRCODE_DATA_CORRUPTED (PostgreSQL error code)

- Called from (representative examples):
  - [toast_decompress_datum_slice](../t/toast_decompress_datum_slice.md) (primary caller for TOAST decompression)

## Notes and Other Information
- Requires LZ4 library version 1.8.3 or later for partial decompression support
- Falls back to full decompression for older LZ4 versions, maintaining compatibility
- Located in src/backend/access/common/toast_compression.c:215-253
- Part of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system
- Returns NULL and issues NO_LZ4_SUPPORT error if PostgreSQL was compiled without LZ4 support
- Validates decompression success and reports DATA_CORRUPTED error for invalid compressed data
- Memory allocation size includes both the slice length and varlena header (VARHDRSZ)
- The function is primarily used for optimizing queries that only need to examine the beginning of large compressed values