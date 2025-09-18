# pgstat_bgwriter_snapshot_cb

## Location
src/backend/utils/activity/pgstat_bgwriter.c: 94 - 109

## Overview
Creates a snapshot of background writer statistics by copying current statistics from shared memory and compensating for reset offsets to provide accurate cumulative values.

## Definition
void pgstat_bgwriter_snapshot_cb(void)

## Detailed Description
This callback function is responsible for taking a consistent snapshot of background writer statistics from shared memory into the local statistics snapshot structure. The function implements a two-phase process:

1. **Statistics Copy Phase**: Uses pgstat_copy_changecounted_stats() to atomically copy the current statistics from shared memory using the changecount mechanism to ensure consistency during concurrent updates.

2. **Reset Compensation Phase**: Acquires a shared lock on the statistics structure and applies reset offset compensation. This ensures that the snapshot reflects cumulative statistics since the last reset, rather than raw counters that may have been reset.

The reset compensation is necessary because PostgreSQL statistics can be reset during operation, and this function ensures that the snapshot presents a consistent view that accounts for these resets.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - PgStatShared_BgWriter (type for shared memory statistics structure)
  - PgStat_BgWriterStats (type for statistics data)
  - [pgstat_copy_changecounted_stats](pgstat_copy_changecounted_stats.md) (atomic copy function with changecount)
  - LWLockAcquire (lightweight lock acquisition)
  - LWLockRelease (lightweight lock release)
  - LW_SHARED (shared lock mode constant)
  - memcpy (memory copy function)
- Called from (representative examples):
  - SH_DECLARE (part of shared hash table declaration mechanism)

## Notes and Other Information
- This is a callback function, likely registered with the statistics snapshot system
- The function uses both changecount-based atomic copying and explicit locking for different phases
- Reset offset compensation is applied using the BGWRITER_COMP macro for fields: buf_written_clean, maxwritten_clean, and buf_alloc
- The shared lock is used only for the brief period needed to copy the reset offset, minimizing lock contention
- The combination of changecount copying and reset compensation ensures that snapshot readers get consistent, cumulative statistics values