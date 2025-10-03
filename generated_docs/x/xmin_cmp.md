# xmin_cmp

## Location
[src/backend/utils/time/snapmgr.c:880-913](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L880-L913)

## Overview
A comparison function for the RegisteredSnapshots pairing heap that orders snapshots by their xmin (minimum transaction ID), ensuring the snapshot with the smallest xmin is at the top of the heap.

## Definition

```c
static int
xmin_cmp(const pairingheap_node *a, const pairingheap_node *b, void *arg)
```
## Detailed Description
This function serves as a comparison callback for PostgreSQL's pairing heap data structure used to maintain registered snapshots. It implements a three-way comparison that orders SnapshotData objects by their xmin field in ascending order. The function extracts SnapshotData structures from pairing heap nodes using container_of-style macros and compares their xmin values using PostgreSQL's transaction ID comparison functions that handle modular arithmetic properly.

The ordering ensures that snapshots with older (smaller) xmin values are prioritized at the top of the heap, which is crucial for snapshot management and garbage collection decisions.

## Parameters / Member Variables
- `*a`: Pointer to the first pairing heap node containing a SnapshotData structure
- `*b`: Pointer to the second pairing heap node containing a SnapshotData structure
- `*arg`: Unused void pointer argument (required by pairing heap callback interface)
## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_const_container
  - [SnapshotData](../S/SnapshotData.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdFollows](../T/TransactionIdFollows.md)
- Called from (representative examples):
  - RegisteredSnapshots pairing heap operations (as comparison callback)

## Notes and Other Information
- Returns 1 if snapshot a has smaller xmin than snapshot b
- Returns -1 if snapshot a has larger xmin than snapshot b  
- Returns 0 if both snapshots have equal xmin values
- Used exclusively as a callback function for the RegisteredSnapshots pairing heap
- Critical for maintaining proper snapshot ordering for visibility and cleanup operations