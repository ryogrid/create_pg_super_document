# detoast_external_attr

## Location
src/backend/access/common/detoast.c: 45 - 115

## Overview
A public entry point function that retrieves a toasted value from external storage, returning a datum containing all data internally without relying on external storage or memory.

## Definition


## Detailed Description
This function handles the retrieval and conversion of externally stored data values back into a usable format. It processes different types of external references including:

1. **External on-disk storage**: Values stored in TOAST relations that need to be fetched from disk
2. **Indirect pointers**: References to values stored elsewhere in memory that need to be dereferenced
3. **Expanded objects**: Complex data structures that need to be flattened into standard varlena format
4. **Plain values**: Regular values that don't require special processing

The function ensures that the returned datum contains all necessary data internally and can be safely freed by the caller when the input is an external datum. The result may still be compressed or have a short header, but it will not depend on external storage.

## Parameters / Member Variables
- : A pointer to the varlena structure that may contain external references to be resolved

## Dependencies
- Functions called/Symbols referenced:
  - [toast_fetch_datum](../t/toast_fetch_datum.md): Retrieves data from TOAST relations for on-disk external values
  - VARATT_IS_EXTERNAL_ONDISK: Macro to check if value is stored externally on disk
  - VARATT_IS_EXTERNAL_INDIRECT: Macro to check if value is an indirect pointer
  - VARATT_IS_EXTERNAL_EXPANDED: Macro to check if value is an expanded object
  - VARATT_IS_EXTERNAL: General macro to check if value has any external storage
  - VARATT_EXTERNAL_GET_POINTER: Macro to extract pointer from indirect reference
  - DatumGetEOHP: Converts datum to ExpandedObjectHeader pointer
  - EOH_get_flat_size: Gets the size needed for flattened expanded object
  - EOH_flatten_into: Flattens expanded object into provided buffer
  - VARSIZE_ANY: Gets the total size of a varlena value
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
- Called from (representative examples):
  - [detoast_attr](detoast_attr.md): General detoasting function
  - [detoast_attr_slice](detoast_attr_slice.md): Slice-based detoasting function
  - [toast_flatten_tuple](../t/toast_flatten_tuple.md): Tuple flattening operations
  - [index_form_tuple_context](../i/index_form_tuple_context.md): Index tuple formation

## Notes and Other Information
- The function handles recursive dereferencing for indirect pointers, ensuring nested indirect datums are not allowed
- For indirect pointers, the result is copied into the caller's memory context to ensure it can be safely freed
- For expanded objects, the function flattens them into standard varlena format
- Plain values that don't require external processing are returned as-is
- This is a critical function in PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system for managing large data values