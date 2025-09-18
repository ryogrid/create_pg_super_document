# transientrel_destroy

## Location
src/backend/commands/matview.c: 537 - 553

## Overview
transientrel_destroy is a cleanup callback function that deallocates the DestReceiver object used for transient relation operations.

## Definition
static void transientrel_destroy(DestReceiver *self)

## Detailed Description
This function serves as the destroy callback for a DestReceiver that handles writing tuples to a transient relation. It performs the final cleanup step by deallocating the memory used by the DestReceiver object itself. This function is called after shutdown operations are complete and the DestReceiver is no longer needed, ensuring proper memory management and preventing memory leaks in materialized view operations.

## Parameters / Member Variables
- `self`: DestReceiver pointer to be deallocated (cast from DR_transientrel)

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [CreateTransientRelDestReceiver](../C/CreateTransientRelDestReceiver.md) (callback assignment)

## Notes and Other Information
- Simple wrapper around pfree for consistent memory deallocation
- Called as the final step in DestReceiver lifecycle after shutdown operations
- Part of the standard DestReceiver interface pattern where destroy callbacks handle object deallocation
- Ensures clean memory management for transient relation DestReceiver objects used in materialized view refresh operations