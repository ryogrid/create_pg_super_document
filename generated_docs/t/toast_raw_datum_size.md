# toast_raw_datum_size

## Location
[src/backend/access/common/detoast.c:545-600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/detoast.c#L545-L600)

## Overview
Returns the raw (detoasted) size of a varlena datum including the VARHDRSZ header, handling all possible datum storage formats including external, compressed, expanded, and short forms.

## Definition

```c
struct varlena *attr = (struct varlena *) DatumGetPointer(value);
```
## Detailed Description
This function provides a unified interface for determining the actual size of a varlena datum regardless of how it is currently stored. PostgreSQL uses various storage optimizations for variable-length data including external storage (TOAST), compression, short headers for small values, and expanded in-memory representations. This function abstracts away these implementation details and returns the size that the datum would occupy if it were in its fully materialized, uncompressed form.

The function handles six different storage scenarios: external on-disk storage (TOAST), indirect external storage (pointers to other datums), expanded in-memory representation (for complex types), compressed storage, short header format (for small values), and plain untoasted storage. For each case, it uses the appropriate method to determine the raw size, ensuring consistent behavior across all storage formats.

This function is recursion-safe and handles nested indirect datums by recursively calling itself, though it includes assertions to prevent infinite recursion through nested indirect references.

## Parameters / Member Variables
- : A Datum value whose raw size needs to be determined

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - VARATT_IS_EXTERNAL_ONDISK
  - VARATT_IS_EXTERNAL_INDIRECT
  - VARATT_IS_EXTERNAL_EXPANDED
  - VARATT_IS_COMPRESSED
  - VARATT_IS_SHORT
  - VARATT_EXTERNAL_GET_POINTER
  - [EOH_get_flat_size](../E/EOH_get_flat_size.md)
  - [DatumGetEOHP](../D/DatumGetEOHP.md)
  - VARDATA_COMPRESSED_GET_EXTSIZE
  - VARSIZE_SHORT
  - VARSIZE
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - Assert
- Types used:
  - [varatt_external](../v/varatt_external.md)
  - [varatt_indirect](../v/varatt_indirect.md)
- Called from:
  - Itself (recursive call for indirect datums)
  - [TrackItem](../T/TrackItem.md)
  - [compute_scalar_stats](../c/compute_scalar_stats.md)
  - [build_sorted_items](../b/build_sorted_items.md)
  - [compute_array_stats](../c/compute_array_stats.md)
  - [datum_image_eq](../d/datum_image_eq.md)
  - [datum_image_hash](../d/datum_image_hash.md)
  - [record_image_cmp](../r/record_image_cmp.md)
  - Various text and bytea functions
  - INDIRECT_POINTER_SIZE macro

## Notes and Other Information
- This is a public function accessible throughout the PostgreSQL codebase
- The function is designed to be safe with all datum storage formats and provides consistent results
- For external on-disk datums, it retrieves the size from the stored metadata without accessing the actual TOAST data
- For indirect datums, it prevents infinite recursion through assertions and recursive calls
- For expanded datums, it uses the Expanded Object Header (EOH) interface to get the flat size
- For compressed datums, it adds the header size to the stored external size
- For short header datums, it normalizes the header size to the standard VARHDRSZ
- The function is widely used throughout PostgreSQL for memory management, statistics collection, and data comparison operations
- Return value includes the VARHDRSZ header size for consistency across all datum types

## Simplified Source

```c
Size toast_raw_datum_size(Datum value) {
    struct varlena *attr = (struct varlena *) DatumGetPointer(value);

    if (VARATT_IS_EXTERNAL_ONDISK(attr)) {
        // External TOAST data - get size from stored metadata
        struct varatt_external toast_pointer;
        VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);
        return toast_pointer.va_rawsize;
    }
    else if (VARATT_IS_EXTERNAL_INDIRECT(attr)) {
        // Indirect pointer - recursively get size from target datum
        struct varatt_indirect toast_pointer;
        VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);
        return toast_raw_datum_size(PointerGetDatum(toast_pointer.pointer));
    }
    else if (VARATT_IS_EXTERNAL_EXPANDED(attr)) {
        // Expanded object - get flat size from header
        return EOH_get_flat_size(DatumGetEOHP(value));
    }
    else if (VARATT_IS_COMPRESSED(attr)) {
        // Compressed data - add header size to payload size
        return VARDATA_COMPRESSED_GET_EXTSIZE(attr) + VARHDRSZ;
    }
    else if (VARATT_IS_SHORT(attr)) {
        // Short header format - normalize to standard header size
        return VARSIZE_SHORT(attr) - VARHDRSZ_SHORT + VARHDRSZ;
    }
    else {
        // Plain untoasted datum
        return VARSIZE(attr);
    }
}
```