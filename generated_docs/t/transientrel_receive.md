# transientrel_receive

## Location
src/backend/commands/matview.c: 492 - 519

## Overview
transientrel_receive is a tuple reception callback function that inserts individual tuples into a transient relation using bulk insert optimizations.

## Definition
static bool transientrel_receive(TupleTableSlot *slot, DestReceiver *self)

## Detailed Description
This function serves as the receive callback for a DestReceiver that handles writing tuples to a transient relation. It takes individual tuples from the execution engine and inserts them into the target transient relation using the table_tuple_insert interface. The function is optimized for bulk operations by using the BulkInsertState and insert options configured during startup. It accepts tuple slots of any compatible type, relying on table_tuple_insert to handle type compatibility, which provides flexibility at a slight performance cost compared to using exactly matching slot types.

## Parameters / Member Variables
- `slot`: TupleTableSlot containing the tuple data to be inserted into the transient relation
- `self`: DestReceiver pointer cast to DR_transientrel containing the initialized state for the transient relation

## Dependencies
- Functions called/Symbols referenced:
  - table_tuple_insert
- Called from (representative examples):
  - [CreateTransientRelDestReceiver](../C/CreateTransientRelDestReceiver.md) (callback assignment)

## Notes and Other Information
- Returns true to indicate successful tuple processing to the executor
- The input slot type doesn't need to exactly match the target relation's tuple descriptor - table_tuple_insert handles type conversion
- Skips index maintenance since transient relations are newly created and have no indexes
- Uses bulk insert state and frozen/FSM-skipping options configured in transientrel_startup for optimal performance
- Part of the materialized view refresh infrastructure where transient relations serve as temporary storage