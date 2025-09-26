# tuplestore_puttupleslot

## Location
[src/backend/utils/sort/tuplestore.c:708-729](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L708-L729)

## Overview
A convenience routine that accepts a TupleTableSlot and appends its tuple to the tuplestore without requiring an extra copy operation.

## Definition

```c
void
tuplestore_puttupleslot(Tuplestorestate *state,
						TupleTableSlot *slot)
```
## Detailed Description
This function extracts the tuple data from a TupleTableSlot and stores it in the tuplestore as a MinimalTuple. It serves as a convenience wrapper around the core tuple storage functionality, eliminating the need for callers to manually extract and format tuple data from slots.

The function operates by:
1. Switching to the tuplestore's memory context to ensure proper memory management
2. Creating a MinimalTuple from the slot using ExecCopySlotMinimalTuple
3. Tracking memory usage with the USEMEM macro
4. Delegating the actual storage to tuplestore_puttuple_common
5. Restoring the previous memory context

The tuple is always copied, so the caller doesn't need to preserve the original slot. The function maintains specific read pointer behavior: if the active read pointer is at EOF, it remains at EOF and advances with the write pointer; otherwise, read pointers remain unchanged. This behavior is specifically designed for the convenience of nodeMaterial.c and nodeCtescan.c.

## Parameters / Member Variables
- : Pointer to the Tuplestorestate structure representing the tuplestore
- : TupleTableSlot containing the tuple data to be stored

## Dependencies
- Functions called/Symbols referenced:
  - ExecCopySlotMinimalTuple
  - GetMemoryChunkSpace  
  - USEMEM
  - tuplestore_puttuple_common
- Types used:
  - Tuplestorestate
  - MinimalTuple
  - TupleTableSlot
- Called from (representative examples):
  - ExecMaterial (nodeMaterial.c)
  - CteScanNext (nodeCtescan.c)
  - ExecRecursiveUnion (nodeRecursiveunion.c)
  - TransitionTableAddTuple (trigger.c)

## Notes and Other Information
- The input tuple is always copied, ensuring the caller can safely modify or deallocate the original slot
- Memory allocation occurs in the tuplestore's context to ensure proper cleanup
- Read pointer behavior is specifically designed to minimize repositioning overhead in Material and CTE scan nodes
- This is the preferred interface when working with TupleTableSlots, avoiding manual tuple extraction