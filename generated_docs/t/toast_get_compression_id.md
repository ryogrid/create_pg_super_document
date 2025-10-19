# toast_get_compression_id

## Location
[src/backend/access/common/toast_compression.c:254-284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_compression.c#L254-L284)

## Overview
Extracts the compression ID from a varlena (variable-length) data structure, returning the compression method used or an invalid ID if the data is not compressed.

## Definition

```c
ToastCompressionId
toast_get_compression_id(struct varlena *attr)
```
## Detailed Description
This function analyzes a varlena data structure to determine its compression method. It handles both externally stored TOAST data and inline compressed data. For external TOAST data stored on disk, it extracts the compression method from the external toast pointer. For inline compressed data, it retrieves the compression method from the toast compression header. If the varlena is not compressed, it returns TOAST_INVALID_COMPRESSION_ID.

## Parameters / Member Variables
- `*attr`: Pointer to the varlena structure to examine for compression information
## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_ONDISK
  - VARATT_EXTERNAL_GET_POINTER
  - VARATT_EXTERNAL_IS_COMPRESSED
  - VARATT_EXTERNAL_GET_COMPRESS_METHOD
  - VARATT_IS_COMPRESSED
  - VARDATA_COMPRESSED_GET_COMPRESS_METHOD
  - TOAST_INVALID_COMPRESSION_ID
- Called from (representative examples):
  - [pg_column_compression](../p/pg_column_compression.md)
  - CompressionMethodIsValid

## Notes and Other Information
- Located in src/backend/access/common/toast_compression.c:254-284
- Returns ToastCompressionId type
- Handles both external and inline compressed data scenarios
- Essential for determining compression methods used in PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system

## Simplified Source

```c
ToastCompressionId
toast_get_compression_id(struct varlena *attr)
{
    ToastCompressionId cmid = TOAST_INVALID_COMPRESSION_ID;

    // Check if data is stored externally on disk
    if (VARATT_IS_EXTERNAL_ONDISK(attr))
    {
        struct varatt_external toast_pointer;
        VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

        // Extract compression method from external toast pointer
        if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
            cmid = VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer);
    }
    // Check if data is compressed inline
    else if (VARATT_IS_COMPRESSED(attr))
        cmid = VARDATA_COMPRESSED_GET_COMPRESS_METHOD(attr);

    return cmid;
}
```