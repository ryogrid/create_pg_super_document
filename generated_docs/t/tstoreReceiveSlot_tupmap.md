# tstoreReceiveSlot_tupmap

## Location
[src/backend/executor/tstoreReceiver.c:192-205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/tstoreReceiver.c#L192-L205)

## Overview
A specialized callback function that receives tuples from the executor, applies tuple format conversion using an attribute mapping, and stores the converted tuples in a tuplestore.

## Definition
```c
static bool tstoreReceiveSlot_tupmap(TupleTableSlot *slot, DestReceiver *self)
```

## Detailed Description
This function handles the case where incoming tuples need to be converted from one format to another before storage in the tuplestore. It uses a pre-computed tuple conversion map (created during startup) to transform the input tuple into the target format. The conversion process involves mapping attributes between the source and target tuple descriptors, handling differences in column order, types, or structure.

The function performs a two-step process:
1. Uses `execute_attr_map_slot` to convert the input slot to the target format using the attribute mapping
2. Stores the converted tuple in the tuplestore using the mapped slot

This callback is selected when `tstoreStartupReceiver` determines that a target tuple descriptor has been specified and tuple format conversion is required.

## Parameters / Member Variables
- `slot`: TupleTableSlot containing the source tuple to be converted
- `self`: Pointer to the DestReceiver structure (cast to TStoreState internally)

## Dependencies
- Functions called/Symbols referenced:
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [tuplestore_puttupleslot](tuplestore_puttupleslot.md)
- Called from (representative examples):
  - Set as callback by tstoreStartupReceiver when tuple conversion is needed
  - Referenced in TStoreState structure

## Notes and Other Information
- This function is selected when a target_tupdesc is provided to the tuplestore receiver
- Uses a pre-allocated mapslot (created during startup) to hold converted tuples
- The tupmap (tuple conversion map) is computed once during startup for efficiency
- Cannot be used simultaneously with detoasting operations (mutually exclusive paths)
- Provides format flexibility for cases where the executor output format differs from the desired storage format
- More expensive than tstoreReceiveSlot_notoast but less complex than tstoreReceiveSlot_detoast
- The attribute mapping handles dropped columns, type differences, and column reordering

## Simplified Source

```c
static bool
tstoreReceiveSlot_tupmap(TupleTableSlot *slot, DestReceiver *self)
{
    TStoreState *myState = (TStoreState *) self;

    // Convert tuple using attribute mapping
    execute_attr_map_slot(myState->tupmap->attrMap, slot, myState->mapslot);

    // Store the converted tuple
    tuplestore_puttupleslot(myState->tstore, myState->mapslot);

    return true;
}
```