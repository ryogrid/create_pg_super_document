# BTOneVacInfo

## Location
[src/backend/access/nbtree/nbtutils.c:4367-4371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4367-L4371)

## Overview
BTOneVacInfo is a structure that tracks active B-tree vacuum operations by storing the global index identifier and associated vacuum cycle ID for concurrency management.

## Definition

```c
typedef struct BTOneVacInfo
{
	LockRelId	relid;			/* global identifier of an index */
	BTCycleId	cycleid;		/* cycle ID for its active VACUUM */
} BTOneVacInfo;
```
## Detailed Description
This structure represents a single entry in the shared memory area that tracks currently active B-tree vacuum operations. Each active vacuum operation gets assigned a unique cycle ID to coordinate with other database operations that might need to avoid interfering with ongoing vacuum processes. The structure stores both the global identifier of the index being vacuumed and its associated cycle ID. This information is used to prevent multiple concurrent vacuum operations on the same index and to provide coordination between vacuum and other B-tree operations.

## Parameters / Member Variables
- `relid`: LockRelId structure containing the global identifier (database OID and relation OID) of the index currently being vacuumed
- `cycleid`: BTCycleId value representing the unique cycle identifier assigned to this vacuum operation, used for coordination with other processes
## Dependencies
- Functions called/Symbols referenced:
  - [LockRelId](../L/LockRelId.md) (struct type)
  - BTCycleId (type)
- Called from (representative examples):
  - [BTVacInfo](BTVacInfo.md) (as array member)
  - [_bt_vacuum_cycleid](../b/_bt_vacuum_cycleid.md)
  - [_bt_start_vacuum](../b/_bt_start_vacuum.md)
  - [_bt_end_vacuum](../b/_bt_end_vacuum.md)
  - [BTreeShmemSize](BTreeShmemSize.md)

## Notes and Other Information
- Used within a shared memory area controlled by BtreeVacuumLock for concurrent access protection
- Part of the BTVacInfo structure as a flexible array member to track multiple active vacuum operations
- The system assumes at most one vacuum can be active for any given index at a time
- Cycle IDs are assigned sequentially and help coordinate between vacuum operations and other B-tree activities
- Essential for preventing conflicts during concurrent B-tree maintenance operations