# toast_flatten_tuple

## Location
src/backend/access/heap/heaptoast.c: 350 - 448

## Overview
Flattens a heap tuple by detoasting all out-of-line external attributes, creating a new tuple with all data stored inline.

## Definition
```c
HeapTuple toast_flatten_tuple(HeapTuple tup, TupleDesc tupleDesc)
```

## Detailed Description
The `toast_flatten_tuple` function creates a fully flattened version of a heap tuple by retrieving all externally stored (out-of-line) TOAST values and incorporating them into a new tuple. This process ensures that the resulting tuple contains no external references and can be safely used in contexts where external TOAST access might not be available or desired.

The function processes each variable-length attribute in the tuple, checking if it is externally stored using `VARATT_IS_EXTERNAL`. For external values, it calls `detoast_external_attr` to retrieve the full value from TOAST storage. The function preserves all tuple metadata including identity fields, visibility information, and transaction-related info masks.

Note that this function only handles out-of-line external values - it does not decompress inline compressed values or expand short-header datums, leaving those optimizations intact.

## Parameters / Member Variables
- `tup`: The heap tuple to be flattened (must have external attributes)
- `tupleDesc`: The tuple descriptor describing the tuple structure

## Dependencies
- Functions called/Symbols referenced:
  - heap_deform_tuple
  - detoast_external_attr
  - heap_form_tuple
  - VARATT_IS_EXTERNAL
  - MaxTupleAttributeNumber
  - HEAP_XACT_MASK
  - HEAP2_XACT_MASK
- Called from (representative examples):
  - ExtractReplicaIdentity
  - expanded_record_set_tuple
  - CatalogCacheCreateEntry

## Notes and Other Information
- Expects the caller to have already verified that the tuple has external attributes using HeapTupleHasExternal()
- Does not eliminate compressed or short-header datums, only external references
- Preserves tuple identity fields (t_self, t_tableOid) and visibility information
- Carefully maintains transaction-related info masks from the original tuple
- Memory management: allocates new storage for detoasted values and cleans up temporary allocations
- The resulting tuple is completely self-contained with no external dependencies