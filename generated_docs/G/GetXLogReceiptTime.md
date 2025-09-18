# GetXLogReceiptTime

## Location
src/backend/access/transam/xlogrecovery.c: 4643 - 4659

## Overview
GetXLogReceiptTime returns the time of receipt of the current chunk of XLOG data and indicates whether it was received from streaming replication or from archives.

## Definition
```c
void GetXLogReceiptTime(TimestampTz *rtime, bool *fromStream)
```

## Detailed Description
This function retrieves information about when the current chunk of WAL (Write-Ahead Logging) data was received and its source. It provides two pieces of information through output parameters: the timestamp when the data was received (stored in XLogReceiptTime) and a boolean indicating whether the data came from streaming replication or from archive files. The function includes an assertion that it must be executed within the startup process during recovery, as the relevant state (XLogReceiptTime and XLogReceiptSource) is not exported to shared memory and is only available in the startup process context.

## Parameters / Member Variables
- `rtime`: Output parameter that receives the timestamp when the current XLOG chunk was received
- `fromStream`: Output parameter that receives true if the data came from streaming replication, false if from archives

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for runtime verification)
  - InRecovery (global recovery state flag)
  - XLogReceiptTime (global variable storing receipt timestamp)
  - XLogReceiptSource (global variable storing source type)
  - XLOG_FROM_STREAM (constant for stream source identification)
- Called from (representative examples):
  - [GetStandbyLimitTime](GetStandbyLimitTime.md)
  - [EndOfWalRecoveryInfo](../E/EndOfWalRecoveryInfo.md)

## Notes and Other Information
- Must be called only from the startup process during recovery (enforced by Assert(InRecovery))
- Returns information via output parameters rather than return values
- The relevant state variables (XLogReceiptTime, XLogReceiptSource) are process-local, not shared memory
- Used for determining WAL source and timing for standby coordination and recovery monitoring
- The fromStream boolean helps distinguish between streaming replication and archive-based recovery modes