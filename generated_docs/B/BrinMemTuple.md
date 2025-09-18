BrinMemTuple

## Overview
BrinMemTuple is a structure that represents an in-memory BRIN index tuple, containing metadata and an array of BrinValues for efficient manipulation during BRIN operations.

## Definition
typedef struct BrinMemTuple
{
    bool          bt_placeholder;   /* this is a placeholder tuple */
    bool          bt_empty_range;   /* range represents no tuples */
    BlockNumber   bt_blkno;         /* heap blkno that the tuple is for */
    MemoryContext bt_context;       /* memcxt holding the bt_columns values */
    /* output arrays for brin_deform_tuple: */
    Datum        *bt_values;        /* values array */
    bool         *bt_allnulls;      /* allnulls array */
    bool         *bt_hasnulls;      /* hasnulls array */
    /* not an output array, but must be last */
    BrinValues    bt_columns[FLEXIBLE_ARRAY_MEMBER];
} BrinMemTuple;

## Detailed Description
BrinMemTuple represents the in-memory form of a BRIN index tuple and serves as the primary working structure for BRIN operations. Unlike the on-disk BrinTuple format, BrinMemTuple provides convenient access to individual column values and metadata. The structure includes separate arrays for efficient access to values, null flags, and summary information. The bt_columns flexible array member contains one BrinValues entry per indexed column, allowing the structure to adapt to indexes with varying numbers of columns. This design facilitates efficient tuple manipulation during index construction, updates, and query processing.

## Parameters / Member Variables
- `bt_placeholder`: Boolean flag indicating this is a placeholder tuple used during index operations
- `bt_empty_range`: Boolean flag indicating the page range contains no actual tuples
- `bt_blkno`: Block number in the heap that this index tuple summarizes
- `bt_context`: Memory context used for managing the lifetime of bt_columns values
- `bt_values`: Output array of Datum values used by brin_deform_tuple for efficient access
- `bt_allnulls`: Output array of boolean flags indicating if all values in a column are NULL
- `bt_hasnulls`: Output array of boolean flags indicating if any values in a column are NULL
- `bt_columns`: Flexible array of BrinValues structures, one per indexed column

## Dependencies
- Functions called/Symbols referenced:
  - [BrinValues](BrinValues.md) (embedded structure)
  - FLEXIBLE_ARRAY_MEMBER (macro)
  - BlockNumber (data type)
  - [MemoryContext](../M/MemoryContext.md) (data type)
  - Datum (data type)
- Called from (representative examples):
  - [brin_form_tuple](../b/brin_form_tuple.md)
  - [brin_new_memtuple](../b/brin_new_memtuple.md)
  - [brin_memtuple_initialize](../b/brin_memtuple_initialize.md)
  - [brin_deform_tuple](../b/brin_deform_tuple.md)
  - [union_tuples](../u/union_tuples.md)
  - [add_values_to_range](../a/add_values_to_range.md)
  - [brin_build_empty_tuple](../b/brin_build_empty_tuple.md)

## Notes and Other Information
- Values can only be meaningfully decoded with an appropriate BrinDesc structure
- The structure uses a flexible array member for bt_columns to support variable numbers of indexed columns
- Output arrays (bt_values, bt_allnulls, bt_hasnulls) are populated by brin_deform_tuple for efficient access
- Used extensively throughout BRIN index operations including insertion, querying, and tuple union operations
- The bt_context field manages memory for the variable-sized bt_columns array and associated data
- Placeholder and empty range flags support special cases in BRIN index management