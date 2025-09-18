# GetStandbyLimitTime

## Location
src/backend/storage/ipc/standby.c: 200 - 223

## Overview
Determines the cutoff time at which the standby server should start canceling conflicting transactions based on configured delay settings and WAL data receipt time.

## Definition
```c
static TimestampTz GetStandbyLimitTime(void)
```

## Detailed Description
This static function calculates the maximum time the standby server should wait before taking action against conflicting transactions. It considers the time when WAL data was last received and adds the appropriate delay based on whether the data came from streaming replication or archive recovery. The function returns a timestamp representing the cutoff time, or zero (past time) if configured to wait indefinitely.

## Parameters / Member Variables
- No parameters (void function)
- Returns: TimestampTz - cutoff time for canceling conflicting transactions, or 0 to wait forever

## Dependencies
- Functions called/Symbols referenced:
  - [GetXLogReceiptTime](GetXLogReceiptTime.md) (gets the last WAL data receipt time and source)
  - TimestampTzPlusMilliseconds (adds milliseconds to timestamp)
  - max_standby_streaming_delay (GUC variable for streaming delay)
  - max_standby_archive_delay (GUC variable for archive delay)
- Called from (representative examples):
  - [WaitExceedsMaxStandbyDelay](../W/WaitExceedsMaxStandbyDelay.md) (checks if wait time has exceeded limits)
  - ResolveRecoveryConflictWithLock (resolves lock conflicts during recovery)
  - ResolveRecoveryConflictWithBufferPin (resolves buffer pin conflicts during recovery)

## Notes and Other Information
- Static function, only used within standby.c
- Uses different delay settings based on WAL data source (streaming vs archive)
- A delay of -1 in either max_standby_streaming_delay or max_standby_archive_delay means wait forever
- Returns 0 (past time) when configured to wait indefinitely, allowing immediate conflict resolution
- Central to PostgreSQL's hot standby conflict resolution mechanism
- The cutoff time is calculated as: last_wal_receipt_time + appropriate_delay_setting