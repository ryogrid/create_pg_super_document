# HeapTupleSatisfiesNonVacuumable

## Location
src/backend/access/heap/heapam_visibility.c: 1429 - 1464

## Overview
HeapTupleSatisfiesNonVacuumable determines whether a heap tuple might be visible to some transaction, returning false only if the tuple is surely dead to everyone and thus vacuumable.

## Definition
static bool HeapTupleSatisfiesNonVacuumable(HeapTuple htup, Snapshot snapshot, Buffer buffer)

## Detailed Description
This function serves as an interface to HeapTupleSatisfiesVacuum that can be called via HeapTupleSatisfiesSnapshot, allowing it to be used through a Snapshot mechanism. It implements the SNAPSHOT_NON_VACUUMABLE behavior by checking if a tuple is definitely dead and vacuumable.

The function works by first calling HeapTupleSatisfiesVacuumHorizon to get the vacuum status of the tuple. If the tuple is HEAPTUPLE_RECENTLY_DEAD, it further checks using GlobalVisTestIsRemovableXid to determine if the transaction that made it dead is old enough to be considered truly dead according to the snapshot's visibility test horizon.

The function returns true if the tuple might be visible to some transaction, and false only when it's certain the tuple is dead to all transactions and can be vacuumed.

## Parameters / Member Variables
- `htup`: The heap tuple to check for visibility
- `snapshot`: Snapshot containing the visibility test horizon (snapshot->vistest must be set up)
- `buffer`: Buffer containing the tuple

## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleSatisfiesVacuumHorizon](HeapTupleSatisfiesVacuumHorizon.md)
  - [GlobalVisTestIsRemovableXid](../G/GlobalVisTestIsRemovableXid.md)
  - HTSV_Result (enum type)
  - HEAPTUPLE_RECENTLY_DEAD (enum value)
  - HEAPTUPLE_DEAD (enum value)
- Called from (representative examples):
  - [HeapTupleSatisfiesVisibility](HeapTupleSatisfiesVisibility.md)

## Notes and Other Information
- This is a static function, only accessible within the heapam_visibility.c file
- The function is designed to be callable through the snapshot mechanism, providing a consistent interface for visibility checking
- It specifically implements the SNAPSHOT_NON_VACUUMABLE snapshot type behavior
- The function uses assertions to validate that dead_after is properly set when expected