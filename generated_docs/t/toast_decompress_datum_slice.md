# toast_decompress_datum_slice

## Location
[src/backend/access/common/detoast.c:503-544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/detoast.c#L503-L544)

## Overview
Decompresses only the front portion (prefix) of a compressed varlena datum, optimizing performance when only a partial decompression is needed from compressed TOAST data.

## Definition

```c
static struct varlena *
toast_decompress_datum_slice(struct varlena *attr, int32 slicelength)
```
## Detailed Description
This function provides efficient partial decompression of compressed TOAST data by decompressing only the requested prefix rather than the entire datum. This is particularly useful for operations that only need to access the beginning portion of large compressed values, significantly reducing CPU overhead and memory usage.

The function includes intelligent optimization logic - if the requested slice length equals or exceeds the total decompressed size, it delegates to the full decompression function instead. This optimization prevents potential issues with compression libraries that may malfunction when given output buffer sizes larger than the actual decompressed data size, while also avoiding unnecessary memory allocation.

Like its full decompression counterpart, this function supports multiple compression algorithms and acts as a dispatch mechanism, routing to the appropriate algorithm-specific slice decompression routine.

## Parameters / Member Variables
- `*attr`: Pointer to a varlena structure containing compressed data
- `slicelength`: Number of bytes to decompress from the beginning of the datum
## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_COMPRESSED
  - TOAST_COMPRESS_EXTSIZE
  - TOAST_COMPRESS_METHOD
  - [toast_decompress_datum](toast_decompress_datum.md)
  - [pglz_decompress_datum_slice](../p/pglz_decompress_datum_slice.md)
  - [lz4_decompress_datum_slice](../l/lz4_decompress_datum_slice.md)
  - elog
  - Assert
- Types used:
  - ToastCompressionId
  - TOAST_PGLZ_COMPRESSION_ID
  - TOAST_LZ4_COMPRESSION_ID
- Called from:
  - [detoast_attr_slice](../d/detoast_attr_slice.md)

## Notes and Other Information
- This is a static function accessible only within the detoast.c compilation unit
- The function includes an assertion to ensure the input is compressed data
- Optimization logic prevents issues with compression libraries that have problems with oversized output buffers
- Only supports prefix decompression (from the front) - cannot decompress arbitrary slices from the middle
- Falls back to full decompression when the slice length exceeds the total decompressed size
- Supports the same compression methods as the full decompression function (PGLZ and LZ4)
- Error handling includes reporting of invalid compression method IDs
- This function is specifically designed for scenarios where offset handling is managed by the caller (detoast_attr_slice)
- Performance benefits are most significant for large compressed values where only a small prefix is needed