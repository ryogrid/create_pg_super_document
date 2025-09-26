# VacDeadItemsInfo

## Location
[src/include/commands/vacuum.h:285-289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/vacuum.h#L285-L289)

## Overview
VacDeadItemsInfo stores supplemental information and constraints for managing dead tuple TID storage during VACUUM operations using TidStore.

## Definition

```c
typedef struct VacDeadItemsInfo
{
	size_t		max_bytes;		/* the maximum bytes TidStore can use */
	int64		num_items;		/* current # of entries */
} VacDeadItemsInfo;
```
## Detailed Description
VacDeadItemsInfo is a lightweight structure that tracks metadata about dead tuple identifier (TID) storage during VACUUM operations. It works in conjunction with TidStore, which is PostgreSQL's mechanism for efficiently storing and managing sets of tuple identifiers that represent dead tuples.

The structure serves two primary purposes:
1. **Memory Management**: Enforces memory limits on TidStore usage through max_bytes
2. **Usage Tracking**: Maintains count of stored dead tuple identifiers through num_items

This information is crucial for VACUUM's memory management strategy, allowing it to:
- Respect memory limits during dead tuple collection
- Track storage efficiency and usage patterns
- Coordinate between parallel vacuum workers
- Make decisions about when to perform index cleanup based on the number of collected dead items

## Parameters / Member Variables
- `max_bytes`: Maximum memory in bytes that the associated TidStore is allowed to consume. This limit helps prevent VACUUM from using excessive memory during dead tuple collection.
- `num_items`: Current number of dead tuple identifiers stored in the TidStore. This count is used to track collection progress and make decisions about when to perform index cleanup operations.
## Dependencies
- Functions called/Symbols referenced:
  - size_t (standard size type)
  - int64 (64-bit integer type)
  - [TidStore](../T/TidStore.md) (implied usage for dead tuple storage)

- Called from (representative examples):
  - [dead_items_alloc](../d/dead_items_alloc.md) (src/backend/access/heap/vacuumlazy.c:2825)
  - [vac_bulkdel_one_index](../v/vac_bulkdel_one_index.md) (src/backend/commands/vacuum.c:2538)
  - [parallel_vacuum_get_dead_items](../p/parallel_vacuum_get_dead_items.md) (src/backend/commands/vacuumparallel.c:465)
  - [parallel_vacuum_reset_dead_items](../p/parallel_vacuum_reset_dead_items.md) (src/backend/commands/vacuumparallel.c:475)
  - [LVRelState](../L/LVRelState.md) (src/backend/access/heap/vacuumlazy.c:188)
  - [PVShared](../P/PVShared.md) (src/backend/commands/vacuumparallel.c:120)

## Notes and Other Information
- Designed to work with TidStore for efficient dead tuple identifier management
- Critical for memory-bounded VACUUM operations, especially on large tables
- Used in both serial and parallel VACUUM operations
- The max_bytes constraint helps prevent memory exhaustion during dead tuple collection
- num_items tracking enables efficient coordination between tuple collection and index cleanup phases
- Part of PostgreSQL's approach to scalable VACUUM operations that can handle tables with millions of dead tuples
- Essential for determining when to switch from collecting dead tuples to performing index cleanup
- Supports vacuum's two-phase approach: collect dead tuple identifiers, then clean indexes and heap