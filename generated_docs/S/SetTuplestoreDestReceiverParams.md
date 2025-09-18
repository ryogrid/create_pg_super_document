# SetTuplestoreDestReceiverParams

## Location
[src/backend/executor/tstoreReceiver.c:266-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/tstoreReceiver.c#L266-L283)

## Overview
Configures the parameters for a tuplestore destination receiver, setting up the target tuplestore, memory context, and various processing options for tuple handling.

## Definition
void SetTuplestoreDestReceiverParams(DestReceiver *self, Tuplestorestate *tStore, MemoryContext tContext, bool detoast, TupleDesc target_tupdesc, const char *map_failure_msg)

## Detailed Description
This function completes the initialization of a tuplestore destination receiver by setting its private configuration parameters. It takes a receiver created by CreateTuplestoreDestReceiver and configures it with the actual tuplestore where data should be stored, the memory context to use, and various processing options. The function supports two main modes of operation: detoasting (expanding compressed/external data) or tuple format conversion, but not both simultaneously.

## Parameters / Member Variables
- self: Pointer to the DestReceiver (actually a TStoreState) to configure
- tStore: The Tuplestorestate object where received tuples will be stored
- tContext: Memory context that contains the tuplestore and should be used for allocations
- detoast: Boolean flag indicating whether to forcibly detoast (expand) compressed or external tuple data
- target_tupdesc: If not NULL, tuples will be converted to match this tuple descriptor format
- map_failure_msg: Error message to display if tuple format conversion fails

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for runtime assertions)
  - TStoreState (structure being configured)
  - DestTuplestore (destination type identifier for validation)
- Called from (representative examples):
  - [PersistHoldablePortal](../P/PersistHoldablePortal.md) (in portalcmds.c for cursor persistence)
  - [FillPortalStore](../F/FillPortalStore.md) (in pquery.c for result storage)

## Notes and Other Information
- Contains an assertion to prevent simultaneous use of detoast and target_tupdesc options, as no current caller needs this combination
- Validates that the receiver is actually a tuplestore type by checking the mydest field
- The function assumes the receiver was properly initialized by CreateTuplestoreDestReceiver
- Part of a two-stage initialization pattern: create the receiver, then set its parameters
- Essential for configuring how the tuplestore receiver will process and store incoming tuples
- The memory context parameter is important for proper memory management during tuple storage operations