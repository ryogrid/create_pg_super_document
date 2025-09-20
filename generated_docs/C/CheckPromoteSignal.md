# CheckPromoteSignal

## Location
[src/backend/access/transam/xlogrecovery.c:4464-4478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4464-L4478)

## Overview
Validates the presence of a promotion signal file to determine if a standby promotion request has been issued.

## Definition

```c
struct stat stat_buf;
```
## Detailed Description
This function performs a file system check to validate whether a promotion signal file exists, confirming that a promotion request has been made for the standby server. It uses the  system call to check for the existence of  without actually reading or modifying the file. This approach is lightweight and efficient, providing a simple boolean response about the promotion signal's presence. The function serves as a secondary validation mechanism in the promotion detection process, typically called after  indicates a potential promotion signal. It's designed to be called frequently during recovery operations without significant performance impact.

## Parameters / Member Variables
This function takes no parameters and returns a boolean value indicating whether the promote signal file exists.

## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md) (system call)
  - PROMOTE_SIGNAL_FILE (macro constant)
- Called from (representative examples):
  - [CheckForStandbyTrigger](CheckForStandbyTrigger.md)
  - [process_pm_pmsignal](../p/process_pm_pmsignal.md)
  - [EndOfWalRecoveryInfo](../E/EndOfWalRecoveryInfo.md) (through header inclusion)

## Notes and Other Information
- This function has public visibility, declared in xlogrecovery.h
- Uses stat() system call for efficient file existence checking without file I/O
- Returns true if the promote signal file exists, false otherwise
- Lightweight operation suitable for frequent calls during recovery
- Part of the two-tier promotion signal detection mechanism
- Does not perform any file cleanup - that's handled by RemovePromoteSignalFiles()
- Located at src/backend/access/transam/xlogrecovery.c:4464-4478