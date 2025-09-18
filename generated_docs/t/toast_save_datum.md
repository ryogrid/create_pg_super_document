# toast_save_datum

## Location
src/backend/access/common/toast_internals.c: 119 - 384

## Overview
Stores a large varlena datum into the secondary toast relation by splitting it into chunks, and returns a pointer datum that references the stored data.

## Definition


## Detailed Description
This function handles the storage of large data values (TOASTed values) into PostgreSQL's secondary toast table. It takes a varlena datum that exceeds inline storage limits and splits it into fixed-size chunks stored as separate tuples in the associated toast relation. The function creates a toast pointer structure that contains metadata about the stored value, including its original size, compression information, and location identifiers.

The function handles several complex scenarios including table rewrites during operations like CLUSTER, where it preserves existing toast value OIDs to maintain referential integrity. It also optimizes for cases where the same toast value might be referenced multiple times during rewrite operations by detecting existing values and avoiding duplicate storage.

Data is stored in chunks up to TOAST_MAX_CHUNK_SIZE bytes each, with each chunk stored as a separate tuple containing the value ID, chunk sequence number, and chunk data. The function maintains proper indexing on the toast relation and ensures transactional consistency.

## Parameters / Member Variables
- : The main relation being worked with (not the toast relation itself)
- : The datum containing the varlena data to be stored in toast storage  
- : Optional pointer to previous external toast value (used during table rewrites for OID preservation)
- : Options passed to heap_insert() when storing toast row tuples

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [toast_open_indexes](toast_open_indexes.md)
  - [toast_close_indexes](toast_close_indexes.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [toastrel_valueid_exists](toastrel_valueid_exists.md)
  - [toastid_valueid_exists](toastid_valueid_exists.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [heap_insert](../h/heap_insert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [index_insert](../i/index_insert.md)
  - [palloc](../p/palloc.md)
  - memcpy
  - VARATT_EXTERNAL_GET_POINTER
  - SET_VARTAG_EXTERNAL
- Called from (representative examples):
  - [toast_tuple_externalize](toast_tuple_externalize.md)

## Notes and Other Information
- Splits large values into chunks of maximum TOAST_MAX_CHUNK_SIZE bytes each
- Handles different varlena formats: short headers, compressed data, and regular format
- During table rewrite operations, attempts to preserve existing toast value OIDs when possible
- Creates proper index entries for all ready indexes on the toast relation
- Maintains transactional locks on toast relation until commit to prevent concurrent reindex conflicts
- The returned toast pointer contains va_toastrelid, va_valueid, va_rawsize, and va_extinfo fields
- Optimizes storage by detecting and avoiding duplicate toast values during rewrites