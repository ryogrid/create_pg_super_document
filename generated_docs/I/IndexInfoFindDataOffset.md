# IndexInfoFindDataOffset

## Location
[src/include/access/itup.h:99-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/itup.h#L99-L117)

## Overview
Calculates the offset to the actual data portion of an IndexTuple based on the tuple's information mask, determining whether space needs to be allocated for null value bitmaps.

## Definition

```c
static inline Size
IndexInfoFindDataOffset(unsigned short t_info)
```
## Detailed Description
This inline function determines the byte offset from the beginning of an IndexTuple to where the actual attribute data begins. The offset calculation depends on whether the tuple contains null values, as indicated by the INDEX_NULL_MASK bit in the t_info parameter.

When no null values are present (INDEX_NULL_MASK bit is not set), the data begins immediately after the IndexTupleData header structure. However, when null values are possible (INDEX_NULL_MASK bit is set), additional space must be allocated for an IndexAttributeBitMapData structure that tracks which attributes are null.

The function is primarily designed to be usable at index_form_tuple time to ensure enough space is allocated for the complete tuple structure. All returned offsets are MAXALIGN-aligned to meet PostgreSQL's alignment requirements.

## Parameters / Member Variables
- : The information mask (unsigned short) from an IndexTuple header that contains various flags including the INDEX_NULL_MASK bit indicating presence of null values

## Dependencies
- Functions called/Symbols referenced:
  - INDEX_NULL_MASK (constant: 0x8000)
  - [IndexTupleData](IndexTupleData.md) (struct)
  - [IndexAttributeBitMapData](IndexAttributeBitMapData.md) (struct)
  - MAXALIGN (macro)
- Called from (representative examples):
  - [index_form_tuple_context](../i/index_form_tuple_context.md)
  - [nocache_index_getattr](../n/nocache_index_getattr.md)
  - [index_deform_tuple](../i/index_deform_tuple.md)
  - [index_getattr](../i/index_getattr.md)
  - [_hash_get_indextuple_hashkey](../h/_hash_get_indextuple_hashkey.md)
  - GinCategoryOffset

## Notes and Other Information
- This is a static inline function defined in src/include/access/itup.h, making it available for high-performance inline expansion
- The function handles the layout optimization where IndexAttributeBitMapData is only included when needed
- The MAXALIGN constraint ensures proper memory alignment for the data portion regardless of whether the null bitmap is present
- The design supports PostgreSQL's space-efficient index tuple format where null bitmaps are conditionally included

## Simplified Source

```c
static inline Size IndexInfoFindDataOffset(unsigned short t_info) {
    // Check if tuple has null values
    if (!(t_info & INDEX_NULL_MASK))
        // No nulls: data starts after basic header
        return MAXALIGN(sizeof(IndexTupleData));
    else
        // Has nulls: data starts after header + null bitmap
        return MAXALIGN(sizeof(IndexTupleData) + sizeof(IndexAttributeBitMapData));
}
```