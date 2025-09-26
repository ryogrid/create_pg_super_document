# HeapTupleTableSlot

## Location
[src/include/executor/tuptable.h:253-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L253-L264)

## Overview
HeapTupleTableSlot is a TupleTableSlot implementation that holds references to HeapTuple structures, supporting efficient access to tuples stored in heap table format.

## Definition
```c
typedef struct HeapTupleTableSlot
{
    pg_node_attr(abstract)
    
    TupleTableSlot base;
    
    HeapTuple   tuple;          /* physical tuple */
    uint32      off;            /* saved state for slot_deform_heap_tuple */
    HeapTupleData tupdata;      /* optional workspace for storing tuple */
} HeapTupleTableSlot;
```

## Detailed Description
HeapTupleTableSlot represents tuples that are stored as HeapTuple structures, which is the standard on-disk storage format for PostgreSQL tables. This slot type maintains a reference to the physical tuple data and provides optimized access to both the tuple header information and individual attribute values. The slot supports lazy deformation of attributes, meaning attribute values are extracted on-demand rather than all at once. The 'off' field tracks the deformation progress for performance optimization, while 'tupdata' provides workspace for tuple operations when needed.

## Parameters / Member Variables
- `base`: The base TupleTableSlot structure containing common slot fields
- `tuple`: Pointer to the HeapTuple containing the actual tuple data
- `off`: Offset tracking the progress of tuple deformation for optimization
- `tupdata`: HeapTupleData workspace for tuple manipulation operations

## Dependencies
- Functions called/Symbols referenced:
  - TupleTableSlot (base structure)
  - HeapTuple
  - HeapTupleData
  - TTSOpsHeapTuple (operations table)
- Called from (representative examples):
  - tts_heap_clear
  - tts_heap_getsomeattrs
  - tts_heap_getsysattr
  - tts_heap_materialize
  - tts_heap_store_tuple

## Notes and Other Information
- Heap tuple slots use TTSOpsHeapTuple as their operations table
- Support efficient access to system attributes (ctid, xmin, xmax, etc.) through the HeapTuple header
- The off field enables incremental tuple deformation for performance optimization
- Can provide both heap tuple and minimal tuple representations
- Supports transaction visibility checks through is_current_xact_tuple operation
- Field number constants are defined for introspection: FIELDNO_HEAPTUPLETABLESLOT_TUPLE, FIELDNO_HEAPTUPLETABLESLOT_OFF
- The tupdata workspace allows for in-place tuple modifications when needed
- Most commonly used for tuples read directly from heap tables or received from remote nodes
- The pg_node_attr(abstract) annotation indicates this is part of the node inheritance hierarchy