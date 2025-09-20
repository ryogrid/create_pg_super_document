# BTParallelScanDesc

## Location
[src/backend/access/nbtree/nbtree.c:83-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L83-L100)

## Overview
BTParallelScanDesc is a pointer type definition that provides a handle to BTParallelScanDescData structures used for managing parallel B-tree index scans.

## Definition

```c
typedef struct BTParallelScanDescData *BTParallelScanDesc;
```
## Detailed Description
BTParallelScanDesc serves as a convenient pointer typedef that abstracts access to the BTParallelScanDescData structure. This type is used throughout the B-tree parallel scanning code to pass references to shared parallel scan state between functions. It follows PostgreSQL's common pattern of creating pointer typedefs for complex structures to improve code readability and maintainability.

The type is used extensively in functions that coordinate parallel B-tree scans, allowing multiple worker processes to share scan state and synchronize their operations safely.

## Parameters / Member Variables
This is a pointer type, so it points to a BTParallelScanDescData structure containing:
- All members of BTParallelScanDescData (btps_scanPage, btps_pageStatus, btps_mutex, btps_cv, btps_arrElems)

## Dependencies
- Functions called/Symbols referenced:
  - [BTParallelScanDescData](BTParallelScanDescData.md) (the underlying structure type)
- Called from (representative examples):
  - [btinitparallelscan](../b/btinitparallelscan.md) (initializes parallel scan descriptor)
  - [btparallelrescan](../b/btparallelrescan.md) (resets parallel scan state)  
  - [_bt_parallel_seize](../b/_bt_parallel_seize.md) (acquires parallel scan control)
  - [_bt_parallel_release](../b/_bt_parallel_release.md) (releases parallel scan control)
  - [_bt_parallel_done](../b/_bt_parallel_done.md) (marks parallel scan as complete)
  - [_bt_parallel_primscan_schedule](../b/_bt_parallel_primscan_schedule.md) (schedules primitive scans)

## Notes and Other Information
This typedef follows PostgreSQL's naming convention where the structure name ends with 'Data' and the pointer typedef uses the same base name without 'Data'. The pointer is typically used to reference shared memory structures that coordinate parallel operations across multiple worker processes. Functions receiving this type should assume the pointed-to structure may be accessed concurrently by other processes and use appropriate synchronization mechanisms.