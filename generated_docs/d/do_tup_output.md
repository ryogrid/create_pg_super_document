# do_tup_output

## Location
[src/backend/executor/execTuples.c:2362-2389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L2362-L2389)

## Overview
Writes a single tuple using the tuple output infrastructure by populating a TupleTableSlot with provided values and sending it to the configured destination receiver.

## Definition
```c
void do_tup_output(TupOutputState *tstate, const Datum *values, const bool *isnull)
```

## Detailed Description
do_tup_output is the core function for outputting individual tuples through the tuple output infrastructure. It takes raw Datum values and null indicators, packages them into a TupleTableSlot, and sends the completed tuple to the destination receiver.

The function follows a clear sequence:
1. Clears the slot to ensure it's in a clean state
2. Copies the provided Datum values and null indicators into the slot's arrays
3. Marks the slot as containing a virtual tuple using ExecStoreVirtualTuple
4. Sends the tuple to the destination via the receiver's receiveSlot function
5. Clears the slot again for cleanup

This approach allows utility commands to easily send structured data to clients using the same mechanisms as regular SELECT queries.

## Parameters / Member Variables
- `tstate`: TupOutputState pointer containing the destination receiver and tuple slot, previously initialized by begin_tup_output_tupdesc
- `values`: Array of Datum values for each column in the tuple, must match the tuple descriptor's column count
- `isnull`: Array of boolean null indicators corresponding to each value, must match the tuple descriptor's column count

## Dependencies
- Functions called/Symbols referenced:
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - memcpy
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
  - [DestReceiver](../D/DestReceiver.md)->receiveSlot
- Called from (representative examples):
  - [SendXlogRecPtrResult](../S/SendXlogRecPtrResult.md) (basebackup_copy.c)
  - [SendTablespaceList](../S/SendTablespaceList.md) (basebackup_copy.c)
  - [do_text_output_multiline](do_text_output_multiline.md) (execTuples.c)
  - [IdentifySystem](../I/IdentifySystem.md) (walsender.c)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (walsender.c)
  - [ShowAllGUCConfig](../S/ShowAllGUCConfig.md) (guc_funcs.c)
  - do_text_output_oneline (executor.h)

## Notes and Other Information
- The function assumes the values and isnull arrays contain exactly natts elements matching the slot's tuple descriptor
- Uses virtual tuple storage which is efficient for temporary tuple data that doesn't need to be materialized
- The slot is cleared both before and after use to maintain clean state
- This is a low-level function typically called by higher-level output functions rather than directly by utility commands
- Part of the tuple output infrastructure designed to unify how utility commands send data to clients