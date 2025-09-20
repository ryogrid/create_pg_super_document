# emode_for_corrupt_record

## Location
[src/backend/access/transam/xlogrecovery.c:4031-4049](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4031-L4049)

## Overview
emode_for_corrupt_record is a utility function that determines the appropriate error reporting level for corrupt WAL records, implementing noise reduction by suppressing repeated complaints about the same record location.

## Definition

```c
static int
emode_for_corrupt_record(int emode, XLogRecPtr RecPtr)
```
## Detailed Description
emode_for_corrupt_record provides intelligent error level management for WAL record corruption scenarios. It implements a sophisticated approach to error reporting that balances the need for diagnostic information with the goal of reducing log noise. The function distinguishes between different WAL sources and applies suppression logic only when reading from pg_wal, as corruption in archive files or streamed records represents more serious issues that should always be reported.

The key behavior is the suppression of repeated error messages for the same record location when the error mode is LOG and the source is XLOG_FROM_PG_WAL. This prevents log spam during recovery scenarios where the same corrupt record might be encountered multiple times as the system retries different sources.

The function maintains state via a static variable to track the last record position that generated a complaint, enabling the suppression logic to work across multiple function calls.

## Parameters / Member Variables
- : Integer representing the initial error mode (typically LOG, PANIC, etc.) that would be used for reporting
- : XLogRecPtr indicating the WAL record position where corruption was detected

## Dependencies
- Functions called/Symbols referenced:
  - XLOG_FROM_PG_WAL (constant comparison)
  - DEBUG1 (error level constant)
  - LOG (error level constant)
- Called from (representative examples):
  - [ReadRecord](../R/ReadRecord.md) (multiple call sites for different error conditions)
  - [XLogPageRead](../X/XLogPageRead.md) (multiple call sites for read errors and corruption)

## Notes and Other Information
- Returns the potentially modified error mode (emode unchanged, or DEBUG1 for suppressed repeated errors)
- Uses static variable lastComplaint to maintain state between calls for suppression logic
- Only applies suppression when source is XLOG_FROM_PG_WAL and error mode is LOG
- [Archive](../A/Archive.md) and streaming sources always report corruption at the original error level since these represent more serious issues
- The function should only be called immediately before ereport() to avoid incorrectly suppressing future legitimate error messages
- The suppression mechanism helps reduce log noise during normal recovery operations where temporary corruption in pg_wal files might be encountered