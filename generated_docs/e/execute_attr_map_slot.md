# execute_attr_map_slot

## Location
[src/backend/access/common/tupconvert.c:192-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupconvert.c#L192-L251)

## Overview
Performs tuple conversion between TupleTableSlots according to an attribute map, providing an efficient slot-to-slot transformation mechanism.

## Definition

```c
TupleTableSlot *
execute_attr_map_slot(AttrMap *attrMap,
					  TupleTableSlot *in_slot,
					  TupleTableSlot *out_slot)
```
## Detailed Description
This function performs tuple conversion between two  objects using a pre-built attribute map. It provides a slot-based alternative to  that works directly with the executor's slot interface, avoiding the need to materialize intermediate tuples.

The conversion process involves extracting all attributes from the input slot, clearing the output slot, mapping values according to the attribute map (with special handling for NULL columns), and storing the result as a virtual tuple in the output slot.

## Parameters
- : Attribute map defining the column correspondence between input and output
- : Input TupleTableSlot containing the source tuple data
- : Output TupleTableSlot where the converted tuple will be stored

## Dependencies
- Functions called/Symbols referenced:
  - slot_getallattrs
  - ExecClearTuple
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
  - [AttrMap](../A/AttrMap.md) (struct)
  - Assert
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md)
  - [AfterTriggerExecute](../A/AfterTriggerExecute.md)
  - [TransitionTableAddTuple](../T/TransitionTableAddTuple.md)
  - [ExecFindPartition](../E/ExecFindPartition.md)
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md)
  - [pgoutput_change](../p/pgoutput_change.md)

## Notes and Other Information
- More efficient than tuple-based conversion as it avoids tuple materialization overhead
- Works directly with slot arrays (tts_values, tts_isnull) rather than heap tuples  
- Handles NULL columns by checking for attrMap->attnums[i] == 0 and setting appropriate NULL values
- The attribute map uses 1-based indexing, so it subtracts 1 when accessing slot arrays
- Clears the output slot before conversion to ensure clean state
- Stores result as a virtual tuple using ExecStoreVirtualTuple
- Extensively used throughout the executor for various tuple routing and conversion scenarios
- Both input and output slots must have valid tuple descriptors and value arrays