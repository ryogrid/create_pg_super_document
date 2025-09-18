# lazy_truncate_heap

## Location
src/backend/access/heap/vacuumlazy.c: 2550 - 2680

## Overview
Attempts to truncate empty pages at the end of a heap relation during vacuum operations, requiring AccessExclusiveLock and careful validation.

## Definition
```c
static void
lazy_truncate_heap(LVRelState *vacrel)
```

## Detailed Description
This function implements heap truncation logic during vacuum operations by removing empty pages from the end of relations. It operates in a loop to handle concurrent activity, first attempting to acquire AccessExclusiveLock with a timeout-based retry mechanism using WaitLatch. The function includes safety checks to detect relation growth during vacuum and uses count_nondeletable_pages to verify that target pages are truly empty. Upon successful validation, it calls RelationTruncate to physically remove the pages, updates vacuum statistics, and releases the exclusive lock. The process may repeat if lock waiters were detected, indicating potential concurrent activity that might create more truncation opportunities.

## Parameters / Member Variables
- `vacrel`: Vacuum relation state containing page counts, relation reference, and truncation context

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [update_vacuum_error_info](../u/update_vacuum_error_info.md)
  - [ConditionalLockRelation](../C/ConditionalLockRelation.md)
  - [WaitLatch](../W/WaitLatch.md)
  - RelationGetNumberOfBlocks
  - [count_nondeletable_pages](../c/count_nondeletable_pages.md)
  - [RelationTruncate](../R/RelationTruncate.md)
  - [UnlockRelation](../U/UnlockRelation.md)
  - PROGRESS_VACUUM_PHASE_TRUNCATE
  - VACUUM_ERRCB_PHASE_TRUNCATE
  - VACUUM_TRUNCATE_LOCK_TIMEOUT
  - VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)

## Notes and Other Information
The function uses a non-blocking approach to lock acquisition to avoid deadlocks, since it already holds lower-grade locks. It includes sophisticated timeout and retry logic to balance truncation success against avoiding disruption to other backends. The function carefully validates that pages are still empty even after acquiring exclusive lock, since concurrent backends may have added tuples during the vacuum process. Progress reporting and error tracking are maintained throughout the operation to provide visibility into potentially long-running truncation operations.