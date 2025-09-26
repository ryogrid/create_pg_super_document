# VirtualTupleTableSlot

## Location
[src/include/executor/tuptable.h:244-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L244-L251)

## Overview
VirtualTupleTableSlot is a specialized TupleTableSlot implementation that stores tuple data only as materialized Datum values without maintaining any underlying physical tuple representation.

## Definition
```c
typedef struct VirtualTupleTableSlot
{
    pg_node_attr(abstract)
    
    TupleTableSlot base;
    
    char       *data;           /* data for materialized slots */
} VirtualTupleTableSlot;
```

## Detailed Description
VirtualTupleTableSlot represents tuples that exist only in their decomposed form as arrays of Datum values in the base TupleTableSlot structure. Unlike other slot types that maintain references to physical tuple representations (HeapTuple, MinimalTuple), virtual slots contain only the attribute values themselves. This makes them ideal for intermediate results in the executor tree, computed expressions, and scenarios where no physical tuple storage is needed. The 'data' field is used when the slot needs to be materialized for operations that require stable memory addresses.

## Parameters / Member Variables
- `base`: The base TupleTableSlot structure containing common slot fields
- `data`: Buffer for materialized slot data when stable memory addresses are required

## Dependencies
- Functions called/Symbols referenced:
  - [TupleTableSlot](../T/TupleTableSlot.md) (base structure)
  - TTSOpsVirtual (operations table)
- Called from (representative examples):
  - [tts_virtual_clear](../t/tts_virtual_clear.md)
  - [tts_virtual_materialize](../t/tts_virtual_materialize.md)
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- Virtual slots use TTSOpsVirtual as their operations table
- They cannot provide heap or minimal tuple representations (those operations return NULL)
- Virtual slots are the most memory-efficient for computed results and intermediate values
- The getsomeattrs operation throws an error since virtual slots always have fully materialized values
- Virtual slots support system attributes through tts_virtual_getsysattr
- Commonly used for projection results, function returns, and expression evaluation results
- The pg_node_attr(abstract) annotation indicates this is part of the node inheritance hierarchy