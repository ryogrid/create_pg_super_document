# CreateTuplestoreDestReceiver

## Location
[src/backend/executor/tstoreReceiver.c:238-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/tstoreReceiver.c#L238-L265)

## Overview
Creates and initializes a new tuplestore destination receiver object that can be used to collect query results and store them in a tuplestore for later retrieval.

## Definition
DestReceiver *CreateTuplestoreDestReceiver(void)

## Detailed Description
This function serves as a factory method for creating tuplestore destination receivers. It allocates memory for a new TStoreState structure and initializes all the callback functions that implement the DestReceiver interface. The receiver can be used by the executor to collect tuples from query execution and store them in a tuplestore data structure. The function sets up all the necessary callbacks but leaves the private fields (like the actual tuplestore reference) to be configured later via SetTuplestoreDestReceiverParams.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (allocates and zeroes memory)
  - TStoreState (structure type being allocated)
  - [tstoreReceiveSlot_notoast](../t/tstoreReceiveSlot_notoast.md) (callback for receiving tuples)
  - [tstoreStartupReceiver](../t/tstoreStartupReceiver.md) (startup callback)
  - [tstoreShutdownReceiver](../t/tstoreShutdownReceiver.md) (shutdown callback)
  - [tstoreDestroyReceiver](../t/tstoreDestroyReceiver.md) (destroy callback)
  - DestTuplestore (destination type identifier)
- Called from (representative examples):
  - [CreateDestReceiver](CreateDestReceiver.md) (in dest.c as part of the destination receiver factory)

## Notes and Other Information
- Returns a DestReceiver pointer that is actually a TStoreState structure cast to the base type
- Uses palloc0 to ensure all fields are initialized to zero/NULL
- The receiveSlot callback is initially set to tstoreReceiveSlot_notoast but may be changed later depending on configuration
- Private fields like tstore, cxt, detoast, target_tupdesc, and map_failure_msg are left uninitialized and must be set via SetTuplestoreDestReceiverParams
- Part of PostgreSQL's destination receiver framework for handling query results
- The mydest field is set to DestTuplestore to identify this as a tuplestore-type receiver