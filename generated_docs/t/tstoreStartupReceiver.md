# tstoreStartupReceiver

## Location
[src/backend/executor/tstoreReceiver.c:56-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/tstoreReceiver.c#L56-L121)

## Overview
Initializes and prepares a tuplestore destination receiver to receive tuples from an executor, setting up the appropriate callback function based on whether detoasting or tuple conversion is needed.

## Definition
```c
static void tstoreStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
```

## Detailed Description
This function serves as the startup method for tuplestore destination receivers. It analyzes the incoming tuple descriptor to determine the optimal processing strategy and configures the receiver accordingly. The function examines whether any columns require detoasting (for variable-length attributes) and whether tuple format conversion is needed based on a target tuple descriptor. Based on this analysis, it selects one of three specialized callback functions and allocates the necessary workspace memory.

The function implements a performance optimization strategy by choosing the most efficient processing path:
- If detoasting is required, it uses `tstoreReceiveSlot_detoast` and allocates workspace arrays
- If tuple conversion is needed, it uses `tstoreReceiveSlot_tupmap` and creates a conversion slot
- Otherwise, it uses the lightweight `tstoreReceiveSlot_notoast` callback

## Parameters / Member Variables
- `self`: Pointer to the DestReceiver structure (cast to TStoreState internally)
- `operation`: Integer indicating the type of operation being performed
- `typeinfo`: TupleDesc describing the format of incoming tuples

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr
  - [convert_tuples_by_position](../c/convert_tuples_by_position.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [tstoreReceiveSlot_detoast](tstoreReceiveSlot_detoast.md)
  - [tstoreReceiveSlot_tupmap](tstoreReceiveSlot_tupmap.md)
  - [tstoreReceiveSlot_notoast](tstoreReceiveSlot_notoast.md)
- Called from (representative examples):
  - [CreateTuplestoreDestReceiver](../C/CreateTuplestoreDestReceiver.md)

## Notes and Other Information
- This is a static function used internally within the tuplestore receiver implementation
- The function performs intelligent optimization by analyzing tuple characteristics upfront
- Detoasting and tuple conversion are mutually exclusive operations (Assert(!myState->tupmap) when needtoast is true)
- Memory allocation uses the receiver's memory context for proper cleanup
- The function sets up workspace arrays (outvalues, tofree) only when detoasting is required