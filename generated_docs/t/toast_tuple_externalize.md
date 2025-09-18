# toast_tuple_externalize

## Location
src/backend/access/table/toast_helper.c: 256 - 274

## Overview
Moves a large attribute to external storage in the TOAST table, replacing the original value with a pointer reference in the main tuple.

## Definition


## Detailed Description
This function handles the externalization of a large attribute by moving its data to the associated TOAST table and replacing it with a pointer in the main tuple. This is the final stage of the TOAST process when compression either failed or didn't provide sufficient space savings.

The function saves the attribute data to external storage using toast_save_datum(), which handles chunking the data into appropriately-sized pieces for the TOAST table. The original attribute value is replaced with a TOAST pointer that contains the necessary information to retrieve the externalized data later.

After externalization, the attribute is marked as TOASTCOL_IGNORE since no further TOAST processing is needed for it. The function also handles proper memory cleanup of the original value when necessary.

## Parameters / Member Variables
- : ToastTupleContext containing the tuple data and metadata
- : Index of the attribute to externalize (0-based array index)
- : Options controlling the externalization process (passed to toast_save_datum)

## Dependencies
- Functions called/Symbols referenced:
  - [toast_save_datum](toast_save_datum.md)
  - [pfree](../p/pfree.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - TOASTCOL_IGNORE
  - TOASTCOL_NEEDS_FREE
  - TOAST_NEEDS_CHANGE
  - TOAST_NEEDS_FREE
- Called from (representative examples):
  - [heap_toast_insert_or_update](../h/heap_toast_insert_or_update.md) (called during externalization phase)

## Notes and Other Information
- This is the last resort in TOAST processing, used when compression cannot reduce the attribute size sufficiently
- The externalized data is stored in chunks in the relation's TOAST table
- The original attribute is replaced with a pointer structure containing metadata for retrieval
- Marks the attribute as TOASTCOL_IGNORE to prevent further processing
- Part of PostgreSQL's strategy to keep the main table rows reasonably sized while supporting very large attribute values
- The options parameter can control aspects like compression of externalized data