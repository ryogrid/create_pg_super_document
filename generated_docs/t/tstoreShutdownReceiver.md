# tstoreShutdownReceiver

## Location
[src/backend/executor/tstoreReceiver.c:206-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/tstoreReceiver.c#L206-L228)

## Overview
Performs cleanup operations when a tuplestore destination receiver is being shut down, releasing all allocated workspace memory and resources.

## Definition
```c
static void tstoreShutdownReceiver(DestReceiver *self)
```

## Detailed Description
This function serves as the cleanup method for tuplestore destination receivers, systematically releasing all resources that were allocated during the receiver's lifetime. It handles the deallocation of various types of workspace memory that may have been allocated depending on the processing mode selected during startup:

1. **Detoasting workspace**: Releases outvalues and tofree arrays used for detoasting operations
2. **Conversion mapping**: Frees the tuple conversion map using free_conversion_map
3. **Mapping slot**: Properly drops the single tuple table slot used for tuple conversion

The function safely handles cases where some resources may not have been allocated, checking for NULL pointers before attempting to free memory. This ensures proper cleanup regardless of which processing path was selected during startup.

## Parameters / Member Variables
- `self`: Pointer to the DestReceiver structure (cast to TStoreState internally)

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
  - [free_conversion_map](../f/free_conversion_map.md)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
- Called from (representative examples):
  - [CreateTuplestoreDestReceiver](../C/CreateTuplestoreDestReceiver.md)

## Notes and Other Information
- This is a static function used as a cleanup callback within the tuplestore receiver framework
- Handles cleanup for all three processing modes (notoast, detoast, tupmap) in a unified manner
- Sets all freed pointers to NULL to prevent double-free errors
- The order of cleanup operations ensures proper resource deallocation
- Part of the standard DestReceiver lifecycle: startup -> receive -> shutdown
- Essential for preventing memory leaks in long-running queries or repeated operations