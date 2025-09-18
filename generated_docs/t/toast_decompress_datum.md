# toast_decompress_datum

## Location
src/backend/access/common/detoast.c: 471 - 502

## Overview
Decompresses a compressed varlena datum by determining the compression method used and delegating to the appropriate decompression routine.

## Definition


## Detailed Description
This function serves as a dispatch mechanism for decompressing TOAST-ed data that has been compressed using various compression algorithms supported by PostgreSQL. It examines the compression header of the input datum to determine which compression method was used (such as PGLZ or LZ4) and then calls the appropriate algorithm-specific decompression function.

The function acts as an abstraction layer that hides the complexity of multiple compression algorithms from the calling code, providing a unified interface for decompression regardless of the underlying compression method. This design allows PostgreSQL to support multiple compression algorithms while maintaining compatibility and extensibility.

## Parameters / Member Variables
- : Pointer to a varlena structure containing compressed data that needs to be decompressed

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_COMPRESSED
  - TOAST_COMPRESS_METHOD
  - [pglz_decompress_datum](../p/pglz_decompress_datum.md)
  - [lz4_decompress_datum](../l/lz4_decompress_datum.md)
  - elog
  - Assert
- Types used:
  - ToastCompressionId
  - TOAST_PGLZ_COMPRESSION_ID
  - TOAST_LZ4_COMPRESSION_ID
- Called from:
  - [detoast_attr](../d/detoast_attr.md)
  - [detoast_attr_slice](../d/detoast_attr_slice.md)
  - [toast_decompress_datum_slice](toast_decompress_datum_slice.md)

## Notes and Other Information
- This is a static function accessible only within the detoast.c compilation unit
- The function includes an assertion to ensure the input is indeed compressed data
- Currently supports two compression methods: PGLZ (PostgreSQL's original compression) and LZ4 (faster compression/decompression)
- Uses a switch statement for extensibility - new compression methods can be easily added
- Error handling includes a default case that reports invalid compression method IDs
- The function design allows for future expansion of compression algorithms without breaking existing code
- Compression method identification is stored in the datum header for efficient dispatch