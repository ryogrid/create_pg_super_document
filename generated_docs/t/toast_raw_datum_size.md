# toast_raw_datum_size

## Location
src/backend/access/common/detoast.c: 545 - 600

## Overview
Returns the raw (detoasted) size of a varlena datum including the VARHDRSZ header, handling all possible datum storage formats including external, compressed, expanded, and short forms.

## Definition


## Detailed Description
This function provides a unified interface for determining the actual size of a varlena datum regardless of how it is currently stored. PostgreSQL uses various storage optimizations for variable-length data including external storage (TOAST), compression, short headers for small values, and expanded in-memory representations. This function abstracts away these implementation details and returns the size that the datum would occupy if it were in its fully materialized, uncompressed form.

The function handles six different storage scenarios: external on-disk storage (TOAST), indirect external storage (pointers to other datums), expanded in-memory representation (for complex types), compressed storage, short header format (for small values), and plain untoasted storage. For each case, it uses the appropriate method to determine the raw size, ensuring consistent behavior across all storage formats.

This function is recursion-safe and handles nested indirect datums by recursively calling itself, though it includes assertions to prevent infinite recursion through nested indirect references.

## Parameters / Member Variables
- : A Datum value whose raw size needs to be determined

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetPointer
  - VARATT_IS_EXTERNAL_ONDISK
  - VARATT_IS_EXTERNAL_INDIRECT
  - VARATT_IS_EXTERNAL_EXPANDED
  - VARATT_IS_COMPRESSED
  - VARATT_IS_SHORT
  - VARATT_EXTERNAL_GET_POINTER
  - EOH_get_flat_size
  - DatumGetEOHP
  - VARDATA_COMPRESSED_GET_EXTSIZE
  - VARSIZE_SHORT
  - VARSIZE
  - PointerGetDatum
  - Assert
- Types used:
  - varatt_external
  - varatt_indirect
- Called from:
  - Itself (recursive call for indirect datums)
  - TrackItem
  - compute_scalar_stats
  - build_sorted_items
  - compute_array_stats
  - datum_image_eq
  - datum_image_hash
  - record_image_cmp
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