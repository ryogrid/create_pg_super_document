# HeapTupleGetUpdateXid

## Location
[src/backend/access/heap/heapam.c:7558-7573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L7558-L7573)

## Overview
Public function that extracts the updating transaction ID from a heap tuple header, serving as a convenient wrapper around MultiXactIdGetUpdateXid.

## Definition
```c
TransactionId HeapTupleGetUpdateXid(HeapTupleHeader tuple)
```

## Detailed Description
This function provides a simple interface to extract the updating transaction ID from a HeapTupleHeader. It serves as a wrapper around MultiXactIdGetUpdateXid, automatically extracting the raw xmax value and infomask from the tuple header and passing them to the lower-level function. This function assumes the caller has already verified that the tuple contains an update (not lock-only) by checking the appropriate hint bits.

The function is widely used throughout the heap access method and visibility checking code where the updating transaction ID needs to be determined from a tuple header.

## Parameters / Member Variables
- `tuple`: Pointer to the HeapTupleHeader from which to extract the updating transaction ID

## Dependencies
- Functions called/Symbols referenced:
  - [MultiXactIdGetUpdateXid](../M/MultiXactIdGetUpdateXid.md)
  - HeapTupleHeaderGetRawXmax
- Types used:
  - HeapTupleHeader
  - TransactionId
- Called from (representative examples):
  - [heap_update](../h/heap_update.md)
  - [HeapTupleSatisfiesSelf](HeapTupleSatisfiesSelf.md)
  - [HeapTupleSatisfiesUpdate](HeapTupleSatisfiesUpdate.md)
  - [HeapTupleSatisfiesDirty](HeapTupleSatisfiesDirty.md)
  - [HeapTupleSatisfiesMVCC](HeapTupleSatisfiesMVCC.md)
  - [HeapTupleSatisfiesVacuumHorizon](HeapTupleSatisfiesVacuumHorizon.md)
  - [HeapTupleHeaderIsOnlyLocked](HeapTupleHeaderIsOnlyLocked.md)
  - [HeapTupleSatisfiesHistoricMVCC](HeapTupleSatisfiesHistoricMVCC.md)
  - HeapTupleHeaderGetUpdateXid

## Notes and Other Information
- Public function exported for use across the codebase
- Assumes caller has verified the tuple contains an update (hint bits checked)
- Related to HeapTupleHeaderGetUpdateXid, which can be used without previously checking hint bits
- Widely used in visibility checking and heap manipulation operations
- Returns the result of MultiXactIdGetUpdateXid applied to the tuple's xmax and infomask
- Does not perform hint bit validation - caller responsibility