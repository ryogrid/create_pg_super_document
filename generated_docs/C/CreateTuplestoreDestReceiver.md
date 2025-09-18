# CreateTuplestoreDestReceiver

## Location
src/backend/executor/tstoreReceiver.c: 238 - 265

## Overview
Creates and initializes a new tuplestore destination receiver object that can be used to collect query results and store them in a tuplestore for later retrieval.

## Definition
DestReceiver *CreateTuplestoreDestReceiver(void)

## Detailed Description
This function serves as a factory method for creating tuplestore destination receivers. It allocates memory for a new TStoreState structure and initializes all the callback functions that implement the DestReceiver interface. The receiver can be used by the executor to collect tuples from query execution and store them in a tuplestore data structure. The function sets up all the necessary callbacks but leaves the private fields (like the actual tuplestore reference) to be configured later via SetTuplestoreDestReceiverParams.

## Parameters / Member Variables
- No parameters - this is a parameterless factory function

## Dependencies
- Functions called/Symbols referenced:
  - palloc0 (allocates and zeroes memory)
  - TStoreState (structure type being allocated)
  - tstoreReceiveSlot_notoast (callback for receiving tuples)
  - tstoreStartupReceiver (startup callback)
  - tstoreShutdownReceiver (shutdown callback)
  - tstoreDestroyReceiver (destroy callback)
  - DestTuplestore (destination type identifier)
- Called from (representative examples):
  - CreateDestReceiver (in dest.c as part of the destination receiver factory)

## Notes and Other Information
- Returns a DestReceiver pointer that is actually a TStoreState structure cast to the base type
- Uses palloc0 to ensure all fields are initialized to zero/NULL
- The receiveSlot callback is initially set to tstoreReceiveSlot_notoast but may be changed later depending on configuration
- Private fields like tstore, cxt, detoast, target_tupdesc, and map_failure_msg are left uninitialized and must be set via SetTuplestoreDestReceiverParams
- Part of PostgreSQL's destination receiver framework for handling query results
- The mydest field is set to DestTuplestore to identify this as a tuplestore-type receiver