# begin_tup_output_tupdesc

## Location
[src/backend/executor/execTuples.c:2342-2361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L2342-L2361)

## Overview
Initializes a TupOutputState structure for sending tuples to a destination as SELECT results, used by utility commands that need to project tuples directly.

## Definition
```c
TupOutputState *begin_tup_output_tupdesc(DestReceiver *dest, TupleDesc tupdesc, const TupleTableSlotOps *tts_ops)
```

## Detailed Description
begin_tup_output_tupdesc creates and initializes a TupOutputState structure that allows utility commands to send tuples to a destination (such as the frontend) as if they were SELECT query results. This function is designed for commands that need to project tuples directly to the destination without requiring full table function capability.

The function allocates a new TupOutputState structure and populates it with:
1. A single TupleTableSlot created using the provided tuple descriptor and slot operations
2. A reference to the destination receiver
3. Calls the destination's startup function with CMD_SELECT command type

This infrastructure is commonly used by utility commands like EXPLAIN and SHOW ALL that need to present structured data as query results.

## Parameters / Member Variables
- `dest`: DestReceiver pointer specifying where the tuples should be sent (typically the frontend client)
- `tupdesc`: TupleDesc describing the structure and types of tuples that will be output
- `tts_ops`: TupleTableSlotOps pointer specifying the slot operations to use for the tuple table slot

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - CMD_SELECT
  - [DestReceiver](../D/DestReceiver.md)->rStartup
- Called from (representative examples):
  - [SendXlogRecPtrResult](../S/SendXlogRecPtrResult.md) (basebackup_copy.c)
  - [SendTablespaceList](../S/SendTablespaceList.md) (basebackup_copy.c)
  - [ExplainQuery](../E/ExplainQuery.md) (explain.c)
  - [ExecuteCallStmt](../E/ExecuteCallStmt.md) (functioncmds.c)
  - [IdentifySystem](../I/IdentifySystem.md) (walsender.c)
  - [ShowGUCConfigOption](../S/ShowGUCConfigOption.md) (guc_funcs.c)
  - [ShowAllGUCConfig](../S/ShowAllGUCConfig.md) (guc_funcs.c)

## Notes and Other Information
- This function is part of a tuple output infrastructure specifically designed for utility commands
- The created TupOutputState should be cleaned up using end_tup_output when output is complete
- The function automatically calls the destination's startup routine to prepare for receiving tuples
- Used extensively in replication commands, configuration display commands, and explain functionality
- The slot operations parameter allows flexibility in how tuples are stored and manipulated

## Simplified Source

```c
// Simplified version of begin_tup_output_tupdesc
TupOutputState *begin_tup_output_tupdesc(DestReceiver *dest,
                                         TupleDesc tupdesc,
                                         const TupleTableSlotOps *tts_ops) {
    TupOutputState *tstate;

    // Allocate output state structure
    tstate = (TupOutputState *) palloc(sizeof(TupOutputState));

    // Create tuple slot for output operations
    tstate->slot = MakeSingleTupleTableSlot(tupdesc, tts_ops);
    tstate->dest = dest;

    // Initialize destination for receiving tuples
    tstate->dest->rStartup(tstate->dest, (int) CMD_SELECT, tupdesc);

    return tstate;
}
```

Key simplifications made:
- Added clear comments explaining each initialization step
- This function is already minimal, serving as a simple constructor
- Preserved all essential components for tuple output infrastructure
- Maintained the CMD_SELECT startup to prepare destination for tuple reception