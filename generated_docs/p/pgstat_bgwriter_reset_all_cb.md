# pgstat_bgwriter_reset_all_cb

## Location
src/backend/utils/activity/pgstat_bgwriter.c: 79 - 93

## Overview
This function serves as a callback to reset all background writer statistics to their initial state when a statistics reset is requested.

## Definition


## Detailed Description
The  function implements the callback mechanism for resetting background writer statistics. It follows a specific reset protocol that ensures thread-safe operations when resetting statistics in shared memory. The function acquires an exclusive lock on the bgwriter statistics shared memory structure, copies the current statistics to the reset offset (preserving a snapshot of values before reset), and updates the reset timestamp. This ensures that any ongoing statistics collection operations can continue safely while the reset occurs.

## Parameters / Member Variables
- : TimestampTz parameter representing the timestamp when the statistics reset occurred. This timestamp is stored in the statistics structure to track when the last reset happened.

## Dependencies
- Functions called/Symbols referenced:
  -  (structure type for shared memory bgwriter statistics)
  -  (function to safely copy statistics with change counting)
  -  (acquire exclusive lock on shared memory)
  -  (release the acquired lock)
  -  (global shared memory statistics structure)

- Called from (representative examples):
  - Referenced by  (src/backend/utils/activity/pgstat.c:354) as part of the statistics framework callback mechanism

## Notes and Other Information
- This function is located in src/backend/utils/activity/pgstat_bgwriter.c:79-93
- The function follows a specific reset protocol as documented above the PgStatShared_BgWriter structure definition
- Uses exclusive locking to ensure thread-safe reset operations in shared memory
- The reset operation preserves historical data by copying current stats to a reset_offset before clearing them
- The timestamp parameter allows tracking of when statistics were last reset, which is useful for monitoring and administrative purposes
- This callback is part of PostgreSQL's statistics reset framework and is typically invoked when administrative functions request a statistics reset