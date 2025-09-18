# free_attrmap

## Location
src/backend/access/common/attmap.c: 56 - 74

## Overview
A utility function that properly deallocates an attribute map structure and its associated memory, ensuring clean resource management in PostgreSQL's attribute mapping system.

## Definition
```c
void free_attrmap(AttrMap *map)
```

## Detailed Description
The `free_attrmap` function is the complementary cleanup function to `make_attrmap`. It performs a two-step deallocation process: first freeing the internal array of attribute numbers (`attnums`), then freeing the main AttrMap structure itself. This function ensures proper memory management and prevents memory leaks when attribute maps are no longer needed. The order of deallocation is important - the internal array must be freed before the containing structure.

## Parameters / Member Variables
- `map`: Pointer to the AttrMap structure to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (memory deallocation function)
  - [AttrMap](../A/AttrMap.md) (structure type)
- Called from (representative examples):
  - [build_attrmap_by_position](../b/build_attrmap_by_position.md)
  - [build_attrmap_by_name_if_req](../b/build_attrmap_by_name_if_req.md)
  - [free_conversion_map](free_conversion_map.md)
  - [DefineIndex](../D/DefineIndex.md)
  - [MergeAttributes](../M/MergeAttributes.md)
  - `logicalrep_relmap_free_entry`
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md)

## Notes and Other Information
- Must be called to prevent memory leaks when an AttrMap is no longer needed
- Follows the proper deallocation order: internal arrays first, then the main structure
- Complementary function to `make_attrmap`
- Used extensively throughout PostgreSQL for cleanup in table operations, index creation, and logical replication
- Located in `src/backend/access/common/attmap.c:56-74`