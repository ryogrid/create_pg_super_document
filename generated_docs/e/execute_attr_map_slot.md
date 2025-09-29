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
  - [slot_getallattrs](../s/slot_getallattrs.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
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

## Simplified Source

```c
TupleTableSlot *execute_attr_map_slot(AttrMap *attrMap,
                                      TupleTableSlot *in_slot,
                                      TupleTableSlot *out_slot)
{
    Datum *invalues, *outvalues;
    bool *inisnull, *outisnull;
    int outnatts;

    // Sanity checks
    Assert(in_slot->tts_tupleDescriptor != NULL &&
           out_slot->tts_tupleDescriptor != NULL);
    Assert(in_slot->tts_values != NULL && out_slot->tts_values != NULL);

    outnatts = out_slot->tts_tupleDescriptor->natts;

    // Extract all values from input slot
    slot_getallattrs(in_slot);

    // Clear output slot before conversion
    ExecClearTuple(out_slot);

    // Get direct access to slot value arrays
    invalues = in_slot->tts_values;
    inisnull = in_slot->tts_isnull;
    outvalues = out_slot->tts_values;
    outisnull = out_slot->tts_isnull;

    // Map values according to attribute map
    for (int i = 0; i < outnatts; i++)
    {
        int j = attrMap->attnums[i] - 1;  // Convert to 0-based indexing

        if (j == -1)  // attrMap->attnums[i] == 0 means NULL column
        {
            outvalues[i] = (Datum) 0;
            outisnull[i] = true;
        }
        else
        {
            outvalues[i] = invalues[j];
            outisnull[i] = inisnull[j];
        }
    }

    // Store result as virtual tuple
    ExecStoreVirtualTuple(out_slot);

    return out_slot;
}
```