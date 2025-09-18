# varattrib_4b

## Location
src/include/varatt.h: 125 - 130

## Overview
The  union represents the structure for 4-byte aligned variable-length attributes (varlena) in PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system, supporting both normal uncompressed and compressed-in-line data formats.

## Definition


## Detailed Description
The  union is a fundamental data structure in PostgreSQL's variable-length attribute storage system. It provides two different layouts for storing variable-length data that requires 4-byte alignment:

1. **Normal varlena format ()**: Used for standard uncompressed variable-length data with a 4-byte header containing length information.

2. **Compressed-in-line format ()**: Used for data that has been compressed and stored inline within the tuple, featuring an additional  field that stores both the original uncompressed size and compression method information.

This union allows PostgreSQL to efficiently handle both compressed and uncompressed variable-length data using the same base structure, with the header bits determining which format is being used.

## Parameters / Member Variables

### va_4byte struct (Normal varlena format):
- : 4-byte header containing length information and format flags
- : Flexible array member containing the actual data payload

### va_compressed struct (Compressed-in-line format):
- : 4-byte header containing length information and compression flags
- : 4-byte field storing original data size (excluding header) and compression method identifier
- : Flexible array member containing the compressed data payload

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER

- Called from (representative examples):
  - VARSIZE_4B (macro for extracting size from 4-byte varlena)
  - SET_VARSIZE_4B (macro for setting size in 4-byte varlena)
  - SET_VARSIZE_4B_C (macro for setting size with compression flag)
  - VARDATA_4B (macro for accessing normal data)
  - VARDATA_4B_C (macro for accessing compressed data)
  - VARHDRSZ_COMPRESSED (macro for compressed header size)
  - VARDATA_COMPRESSED_GET_EXTSIZE (macro for extracting original size)
  - VARDATA_COMPRESSED_GET_COMPRESS_METHOD (macro for extracting compression method)

## Notes and Other Information
- This structure is designed for 4-byte aligned data access, as opposed to the 1-byte aligned  structure
- The union design allows the same memory layout to represent either normal or compressed data efficiently
- The compression support enables PostgreSQL to store larger amounts of data inline within tuples when beneficial
- Part of PostgreSQL's TOAST system for handling oversized attributes
- The structure must maintain specific bit patterns in headers to distinguish between different varlena formats
- Located in 
- The  field uses bit manipulation to pack both size and compression method information into a single 32-bit value