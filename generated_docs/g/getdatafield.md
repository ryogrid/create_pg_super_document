# getdatafield

## Location
src/backend/storage/large_object/inv_api.c: 169 - 210

## Overview
Extracts the data field from a pg_largeobject tuple, handling detoasting if necessary and validating the data size against expected limits.

## Definition


## Detailed Description
This utility function safely extracts the data field from a large object page tuple stored in the pg_largeobject catalog. It handles PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) mechanism by detecting extended attributes and calling detoast_attr when needed. The function validates that the extracted data length falls within acceptable bounds (0 to LOBLKSIZE) and raises a DATA_CORRUPTED error if the size is invalid. It returns the data pointer, actual data length, and a flag indicating whether the caller needs to pfree the returned data pointer.

## Parameters / Member Variables
- : Pointer to the pg_largeobject tuple form containing the data field
- : Output parameter receiving the pointer to the extracted data (bytea *)
- : Output parameter receiving the actual data length in bytes
- : Output parameter indicating whether the caller should pfree the data pointer

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTENDED (macro to check for extended attributes)
  - detoast_attr (function to decompress/retrieve toasted data)
  - VARSIZE (macro to get variable-length data size)
  - LOBLKSIZE (constant defining maximum large object block size)
  - ereport (for error reporting)
- Called from (representative examples):
  - inv_getsize
  - inv_read
  - inv_write
  - inv_truncate

## Notes and Other Information
- Function is static (internal to inv_api.c)
- Handles TOAST decompression transparently
- Validates data integrity by checking size bounds
- Memory management responsibility is communicated via pfreeit flag
- Uses Form_pg_largeobject which provides typed access to tuple fields
- Error handling includes OID and page number in corruption messages for debugging
- Critical for ensuring data integrity in large object operations