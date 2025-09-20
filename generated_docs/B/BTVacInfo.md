# BTVacInfo

## Location
[src/backend/access/nbtree/nbtutils.c:4373-4379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4373-L4379)

## Overview
BTVacInfo is the main shared memory structure that manages vacuum cycle coordination for B-tree indexes by tracking active vacuum operations and assigning unique cycle IDs.

## Definition

```c
typedef struct BTVacInfo
{
	BTCycleId	cycle_ctr;		/* cycle ID most recently assigned */
	int			num_vacuums;	/* number of currently active VACUUMs */
	int			max_vacuums;	/* allocated length of vacuums[] array */
	BTOneVacInfo vacuums[FLEXIBLE_ARRAY_MEMBER];
} BTVacInfo;
```
## Detailed Description
This structure serves as the central coordination mechanism for B-tree vacuum operations in shared memory. It maintains a global cycle counter for assigning unique identifiers to vacuum operations, tracks the number of currently active vacuums, and contains an array of BTOneVacInfo entries that store details about each active vacuum. The structure is protected by BtreeVacuumLock and is used to prevent multiple concurrent vacuum operations on the same index while enabling coordination between vacuum processes and other B-tree operations that might be affected by ongoing maintenance.

## Parameters / Member Variables
- `cycle_ctr`: BTCycleId value representing the most recently assigned cycle ID, incremented for each new vacuum operation to ensure uniqueness
- `num_vacuums`: Integer count of currently active vacuum operations tracked in the vacuums array
- `max_vacuums`: Integer representing the allocated capacity of the vacuums array, typically set to MaxBackends to accommodate the maximum possible concurrent vacuum operations
- `vacuums[FLEXIBLE_ARRAY_MEMBER]`: Flexible array member containing BTOneVacInfo structures, each tracking an active vacuum operation with its index identifier and cycle ID
## Dependencies
- Functions called/Symbols referenced:
  - BTCycleId (type)
  - [BTOneVacInfo](BTOneVacInfo.md) (struct type)
  - FLEXIBLE_ARRAY_MEMBER (macro)
- Called from (representative examples):
  - [BTreeShmemSize](BTreeShmemSize.md)
  - [BTreeShmemInit](BTreeShmemInit.md)

## Notes and Other Information
- Allocated in shared memory during database startup via BTreeShmemInit() and sized by BTreeShmemSize()
- Protected by BtreeVacuumLock (LWLock) for concurrent access control
- The cycle counter is initialized with low-order bits of time() to avoid predictable starting values
- Maximum number of concurrent vacuum operations is limited by MaxBackends
- Essential for preventing race conditions and conflicts during B-tree maintenance operations
- Used globally across all B-tree indexes in the database instance for vacuum coordination