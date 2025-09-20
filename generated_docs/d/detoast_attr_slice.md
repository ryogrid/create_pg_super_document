# detoast_attr_slice

## Location
[src/backend/access/common/detoast.c:205-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/detoast.c#L205-L342)

## Overview
A public entry point function that retrieves a specific slice (substring) of a toasted value from compression or external storage, providing efficient partial data access.

## Definition

```c
struct varlena *
detoast_attr_slice(struct varlena *attr,
				   int32 sliceoffset, int32 slicelength)
```
## Detailed Description
This function provides efficient access to a portion of a potentially large toasted value without necessarily retrieving or decompressing the entire value. It implements several optimizations:

1. **External on-disk storage**: Uses toast_fetch_datum_slice for direct partial retrieval from TOAST relations
2. **Compressed data handling**: For compressed external data, calculates the minimum amount of compressed data needed to decompress the requested slice
3. **Indirect pointers**: Recursively processes indirect references 
4. **Expanded objects**: Flattens expanded objects first, then extracts the slice
5. **Slice boundary validation**: Handles edge cases like offsets beyond data size and negative slice lengths

The function is particularly valuable for accessing substrings of large text values or portions of large binary data without the overhead of retrieving the complete value.

## Parameters / Member Variables
- : A pointer to the varlena structure that may contain the data to be sliced
- : The starting position for the slice (zero-based, must be >= 0)
- : The length of the slice to extract (if < 0, returns everything from sliceoffset to end)

## Dependencies
- Functions called/Symbols referenced:
  - [toast_fetch_datum_slice](../t/toast_fetch_datum_slice.md): Efficiently retrieves a slice from TOAST relations
  - [toast_fetch_datum](../t/toast_fetch_datum.md): Retrieves complete datum from TOAST relations
  - [toast_decompress_datum_slice](../t/toast_decompress_datum_slice.md): Decompresses only the required portion of compressed data
  - [toast_decompress_datum](../t/toast_decompress_datum.md): Decompresses entire compressed datum
  - [detoast_external_attr](detoast_external_attr.md): Handles externally stored attributes
  - pglz_maximum_compressed_size: Calculates maximum compressed size for PGLZ algorithm
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md): Safe integer addition with overflow detection
  - VARATT_IS_EXTERNAL_ONDISK: Checks if value is stored externally on disk
  - VARATT_IS_EXTERNAL_INDIRECT: Checks if value is an indirect pointer
  - VARATT_IS_EXTERNAL_EXPANDED: Checks if value is an expanded object
  - VARATT_IS_COMPRESSED: Checks if value is compressed
  - VARATT_IS_SHORT: Checks if value uses short header format
  - VARATT_EXTERNAL_GET_POINTER: Extracts pointer from external reference
  - VARATT_EXTERNAL_IS_COMPRESSED: Checks if external value is compressed
  - VARATT_EXTERNAL_GET_EXTSIZE: Gets the external size of the value
  - VARATT_EXTERNAL_GET_COMPRESS_METHOD: Gets compression method used
  - VARDATA/VARDATA_SHORT: Macros to access varlena data
  - SET_VARSIZE: Sets the size field of a varlena
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
- Called from (representative examples):
  - [pg_detoast_datum_slice](../p/pg_detoast_datum_slice.md): Main slice detoasting interface in function manager
  - [detoast_attr_slice](detoast_attr_slice.md): Recursive calls for indirect pointers

## Notes and Other Information
- The function includes intelligent optimization for compressed external data: for PGLZ compression, it calculates the minimum compressed data needed for the requested slice, while for LZ4 it fetches the entire compressed data due to API limitations
- Handles integer overflow protection when calculating slice limits using pg_add_s32_overflow
- Automatically adjusts slice parameters when sliceoffset exceeds the actual data size
- For indirect pointers, the function recursively calls itself rather than first dereferencing
- The result is always a newly allocated varlena in standard format, making it safe for the caller to pfree
- Essential for efficient substring operations on large text values and partial access to binary large objects
- Part of PostgreSQL's TOAST system optimizations for handling oversized attributes efficiently