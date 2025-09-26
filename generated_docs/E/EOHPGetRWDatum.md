# EOHPGetRWDatum

## Location
src/include/utils/expandeddatum.h: 139 - 144

## Overview
EOHPGetRWDatum is an inline function that extracts a read-write TOAST pointer from an ExpandedObjectHeader and returns it as a Datum.

## Definition


## Detailed Description
This function provides a convenient way to access the read-write TOAST pointer stored within an expanded object's header. The function takes a pointer to an ExpandedObjectHeader and returns the eoh_rw_ptr field as a Datum using PointerGetDatum. This allows functions to return a read-write pointer to the expanded object without making additional allocations, which is particularly useful for functions that need to modify the expanded object's contents.

The returned Datum represents a TOAST pointer that can be used to access and modify the expanded object. This is part of PostgreSQL's expanded object infrastructure that allows certain data types (like arrays and records) to maintain an "expanded" in-memory representation for efficiency while still being able to produce flattened representations when needed.

## Parameters / Member Variables
- : Pointer to an ExpandedObjectHeader structure containing the expanded object metadata and TOAST pointers

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetDatum
  - ExpandedObjectHeader
- Called from (representative examples):
  - expand_array
  - array_append
  - array_prepend
  - array_set_element_expanded
  - TransferExpandedObject
  - make_expanded_record_from_datum
  - PG_RETURN_EXPANDED_ARRAY
  - ExpandedRecordGetDatum

## Notes and Other Information
- This is an inline function defined in src/include/utils/expandeddatum.h:139-142
- The function accesses the eoh_rw_ptr field which is a standard R/W TOAST pointer stored within the ExpandedObjectHeader
- The counterpart function EOHPGetRODatum provides access to the read-only TOAST pointer
- This function is commonly used in array and record manipulation functions where write access to the expanded object is required
- The returned Datum maintains the same lifespan as the underlying ExpandedObjectHeader's memory context