# toast_tuple_try_compression

## Location
src/backend/access/table/toast_helper.c: 227 - 255

## Overview
Attempts to compress a specific attribute in a tuple and updates the TOAST context with the result, marking incompressible attributes to avoid future compression attempts.

## Definition


## Detailed Description
This function attempts to compress a single attribute using the configured compression algorithm. If compression succeeds and reduces the attribute size sufficiently, it replaces the original value with the compressed version. If compression fails or doesn't provide adequate space savings, the attribute is marked as incompressible to prevent future compression attempts.

The function handles memory management by freeing the old value when compression succeeds and the old value was already allocated for TOAST operations. It also updates the attribute size and sets appropriate flags to indicate that the tuple has been modified and may need additional memory cleanup.

This is a key component of PostgreSQL's TOAST strategy, which attempts compression before externalization to keep data in the main table when possible.

## Parameters / Member Variables
- : ToastTupleContext containing the tuple data and metadata
- : Index of the attribute to attempt compression on (0-based array index)

## Dependencies
- Functions called/Symbols referenced:
  - toast_compress_datum
  - DatumGetPointer
  - pfree
  - VARSIZE
  - TOASTCOL_NEEDS_FREE
  - TOASTCOL_INCOMPRESSIBLE
  - TOAST_NEEDS_CHANGE
  - TOAST_NEEDS_FREE
- Called from (representative examples):
  - heap_toast_insert_or_update (called multiple times during compression phase)

## Notes and Other Information
- Uses the compression method specified in the attribute's tai_compression field
- Marks attributes as TOASTCOL_INCOMPRESSIBLE to optimize future TOAST operations
- Part of PostgreSQL's two-phase TOAST strategy: compression first, then externalization
- Memory management is carefully handled to avoid leaks when replacing values
- The compressed value replaces the original in the ToastTupleContext for subsequent processing
- Compression is attempted using algorithms like PGLZ or LZ4 depending on configuration