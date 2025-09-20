# toast_compress_header

## Location
[src/include/access/toast_internals.h:23-28](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/toast_internals.h#L23-L28)

## Overview
A structure that contains the header information at the start of compressed TOAST data, storing metadata about the compression method and original size of the data.

## Definition

```c
typedef struct toast_compress_header
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		tcinfo;			/* 2 bits for compression method and 30 bits
								 * external size; see va_extinfo */
} toast_compress_header;
```
## Detailed Description
The  structure is a critical component of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) compression system. This structure serves as the header for all compressed TOAST data, providing essential metadata needed for decompression and data management.

The structure is designed with a compact layout where the  field efficiently packs both the compression method identifier (2 bits) and the original uncompressed size (30 bits) into a single 32-bit unsigned integer. This design allows PostgreSQL to support up to 4 different compression methods while still being able to handle data up to approximately 1GB in size (2^30 bytes).

The header is prefixed by a standard varlena header () that follows PostgreSQL's variable-length data storage conventions, allowing the compressed data to be treated as a regular varlena object within the system.

## Parameters / Member Variables
- : Standard PostgreSQL varlena header containing length information and flags. This field should not be accessed directly by application code.
- : A packed 32-bit field containing:
  - Lower 30 bits: Original (uncompressed) size of the data (masked by VARLENA_EXTSIZE_MASK)
  - Upper 2 bits: Compression method identifier (TOAST_PGLZ_COMPRESSION_ID or TOAST_LZ4_COMPRESSION_ID)

## Dependencies
- Functions called/Symbols referenced: None (this is a data structure)
- Used by macros:
  - TOAST_COMPRESS_EXTSIZE (extracts original size from tcinfo)
  - TOAST_COMPRESS_METHOD (extracts compression method from tcinfo)
  - TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD (sets both size and compression method)

## Notes and Other Information
- The structure is defined in 
- The compression method field supports only 2 bits, limiting PostgreSQL to a maximum of 4 compression methods
- Currently supported compression methods are PGLZ (ID=0) and LZ4 (ID=1)
- The 30-bit size field allows for a maximum original data size of approximately 1GB (1073741823 bytes)
- This header format is used consistently across all compressed TOAST entries, ensuring uniform access patterns
- The structure is part of PostgreSQL's internal TOAST system and is not intended for direct manipulation by user code