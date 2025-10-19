# toast_fetch_datum_slice

## Location
[src/backend/access/common/detoast.c:396-470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/detoast.c#L396-L470)

## Overview
Reconstructs a specific segment (slice) of a Datum from chunks stored in a TOAST relation, enabling efficient partial retrieval of large externally stored data without having to fetch the entire value.

## Definition

```c
static struct varlena *
toast_fetch_datum_slice(struct varlena *attr, int32 sliceoffset,
						int32 slicelength)
```
## Detailed Description
This function provides the capability to retrieve only a portion of a large TOAST-ed datum, which is crucial for performance when working with very large values where only a subset of the data is needed. The function supports both compressed and uncompressed external datums, though with restrictions on compressed data - for compressed datums, only prefix slices (starting from offset 0) are supported since compressed data cannot be meaningfully sliced in the middle.

The function handles boundary conditions gracefully, adjusting slice parameters when they exceed the actual data size, and optimizes for cases where the requested slice length is zero. For compressed datums, it accounts for the additional space required by the compression metadata (va_tcinfo) stored as an int32 value at the beginning of the data.

## Parameters / Member Variables
- `*attr`: Pointer to a varlena structure containing the TOAST pointer that references the externally stored data
- `sliceoffset`: Starting byte offset within the external datum from which to begin the slice (must be 0 for compressed datums)
- `slicelength`: Number of bytes to retrieve from the external datum starting at sliceoffset
## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_ONDISK
  - VARATT_EXTERNAL_GET_POINTER
  - VARATT_EXTERNAL_IS_COMPRESSED
  - VARATT_EXTERNAL_GET_EXTSIZE
  - SET_VARSIZE_COMPRESSED
  - SET_VARSIZE
  - [table_open](table_open.md)
  - [table_relation_fetch_toast_slice](table_relation_fetch_toast_slice.md)
  - [table_close](table_close.md)
  - [palloc](../p/palloc.md)
  - elog
  - Assert
- Called from:
  - [detoast_attr_slice](../d/detoast_attr_slice.md)

## Notes and Other Information
- This is a static function accessible only within the detoast.c compilation unit
- For compressed external datums, only prefix slices are supported (sliceoffset must be 0) due to the nature of compression
- The function includes an assertion to enforce the compressed datum slicing restriction
- Boundary checking ensures that slice requests beyond the actual data size are handled gracefully
- For compressed datums, the function automatically accounts for the va_tcinfo metadata overhead
- Memory allocation uses PostgreSQL's palloc system for proper memory context management
- The function can optimize performance by early return when slicelength is zero
- TOAST table access uses AccessShareLock for data consistency during retrieval

## Simplified Source

```c
static struct varlena *toast_fetch_datum_slice(struct varlena *attr, int32 sliceoffset,
                                               int32 slicelength) {
    Relation toastrel;
    struct varlena *result;
    struct varatt_external toast_pointer;
    int32 attrsize;

    // Validate input is an external on-disk datum
    if (!VARATT_IS_EXTERNAL_ONDISK(attr))
        elog(ERROR, "toast_fetch_datum_slice shouldn't be called for non-ondisk datums");

    // Extract toast pointer and validate compressed datum restrictions
    VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);
    Assert(!VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) || 0 == sliceoffset);

    attrsize = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);

    // Adjust slice parameters for boundary conditions
    if (sliceoffset >= attrsize) {
        sliceoffset = 0;
        slicelength = 0;
    }

    // Account for compression metadata overhead
    if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) && slicelength > 0)
        slicelength = slicelength + sizeof(int32);

    // Adjust length if it exceeds available data
    if (((sliceoffset + slicelength) > attrsize) || slicelength < 0)
        slicelength = attrsize - sliceoffset;

    // Allocate result and set appropriate header
    result = (struct varlena *) palloc(slicelength + VARHDRSZ);

    if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
        SET_VARSIZE_COMPRESSED(result, slicelength + VARHDRSZ);
    else
        SET_VARSIZE(result, slicelength + VARHDRSZ);

    // Early return for zero-length slice
    if (slicelength == 0)
        return result;

    // Fetch the specified slice from toast table
    toastrel = table_open(toast_pointer.va_toastrelid, AccessShareLock);
    table_relation_fetch_toast_slice(toastrel, toast_pointer.va_valueid,
                                     attrsize, sliceoffset, slicelength, result);
    table_close(toastrel, AccessShareLock);

    return result;
}
```