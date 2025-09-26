# FileBackupMethod

## Location
[src/include/backup/basebackup_incremental.h:26-28](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/backup/basebackup_incremental.h#L26-L28)

## Overview
An enumeration that specifies how a file should be backed up during incremental backup operations in PostgreSQL.

## Definition

```c
struct IncrementalBackupInfo;
```
## Detailed Description
The  enum is a key component of PostgreSQL's incremental backup system, introduced to optimize backup performance by allowing selective backup of only modified blocks rather than entire files. This enum is used by the  function to determine the appropriate backup strategy for individual database files.

The decision between full and incremental backup is based on several factors:
- File size and block alignment validation
- Fork type (Free Space Map requires full backup)
- Whether the file existed in the prior backup
- Block-level changes tracked through WAL summaries
- File truncation scenarios

## Parameters / Member Variables
-  (value 0): Indicates that the entire file should be backed up. This is chosen when:
  - File size is not properly aligned to BLCKSZ or exceeds RELSEG_SIZE
  - The file is a Free Space Map fork (not properly WAL-logged)
  - File did not exist in the prior backup
  - No block reference table entry exists for the file
  - File has been truncated significantly
  - Block-level analysis determines full backup is more efficient

-  (value 1): Indicates that only modified blocks should be backed up. This is chosen when:
  - File meets size and alignment requirements
  - Block reference table contains entries for the file
  - Only a subset of blocks have been modified since the last backup
  - Incremental backup would be more efficient than full backup

## Dependencies
- Functions called/Symbols referenced:
  - [IncrementalBackupInfo](../I/IncrementalBackupInfo.md) (struct used in context)

- Called from (representative examples):
  - [GetFileBackupMethod](../G/GetFileBackupMethod.md) (returns this enum type)
  - [sendDir](../s/sendDir.md) (in basebackup.c, uses the enum values for backup logic)

## Notes and Other Information
- The enum is defined in 
- This is a critical component for PostgreSQL's incremental backup feature
- The choice between enum values directly impacts backup performance and storage efficiency
- Used primarily during base backup operations when incremental backup information is available
- The decision logic in  implements sophisticated heuristics to balance backup completeness with efficiency
- Full backup is the safer default, used whenever incremental backup conditions are not met