# BackupState

## Location
[src/include/access/xlogbackup.h:21-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogbackup.h#L21-L38)

## Overview
BackupState is a structure that holds comprehensive state information during PostgreSQL backup operations, tracking both the beginning and end phases of backup procedures including incremental backup support.

## Definition
```c
typedef struct BackupState
{
    /* Fields saved at backup start */
    char        name[MAXPGPATH + 1];        /* Backup label name one extra byte for null-termination */
    XLogRecPtr  startpoint;                 /* backup start WAL location */
    TimeLineID  starttli;                   /* backup start TLI */
    XLogRecPtr  checkpointloc;              /* last checkpoint location */
    pg_time_t   starttime;                  /* backup start time */
    bool        started_in_recovery;        /* backup started in recovery? */
    XLogRecPtr  istartpoint;                /* incremental based on backup at this LSN */
    TimeLineID  istarttli;                  /* incremental based on backup on this TLI */

    /* Fields saved at the end of backup */
    XLogRecPtr  stoppoint;                  /* backup stop WAL location */
    TimeLineID  stoptli;                    /* backup stop TLI */
    pg_time_t   stoptime;                   /* backup stop time */
} BackupState;
```

## Detailed Description
The BackupState structure serves as a comprehensive container for managing backup operations in PostgreSQL. It tracks the complete lifecycle of a backup from initiation to completion, storing critical WAL (Write-Ahead Log) coordinates, timeline information, and temporal data necessary for consistent backup and recovery operations.

The structure is designed to support both full and incremental backup scenarios. During backup initiation, it captures the starting WAL position, timeline, checkpoint location, and timestamp. For incremental backups, it additionally stores the base backup coordinates (istartpoint and istarttli) that the incremental backup builds upon.

Upon backup completion, the structure records the ending WAL coordinates and timeline, ensuring that recovery operations can accurately determine the backup's valid range. This information is crucial for point-in-time recovery and ensuring backup consistency.

## Parameters / Member Variables
- `name[MAXPGPATH + 1]`: Backup label name with null-termination support, identifying the specific backup instance
- `startpoint`: WAL (Write-Ahead Log) location where the backup began, marking the consistent starting point
- `starttli`: Timeline ID at backup start, ensuring correct timeline tracking for recovery
- `checkpointloc`: Location of the last checkpoint before backup start, used for consistency validation
- `starttime`: Timestamp when the backup operation was initiated, for audit and management purposes
- `started_in_recovery`: Boolean flag indicating whether the backup was started during recovery mode
- `istartpoint`: LSN (Log Sequence Number) of the base backup for incremental backup operations
- `istarttli`: Timeline ID of the base backup for incremental backup scenarios
- `stoppoint`: WAL location where the backup concluded, defining the backup's end boundary
- `stoptli`: Timeline ID at backup completion, ensuring timeline consistency through the backup process
- `stoptime`: Timestamp when the backup operation completed

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_t (used for starttime and stoptime fields)
  - XLogRecPtr (used for WAL position tracking)
  - TimeLineID (used for timeline management)
  - MAXPGPATH (for backup name buffer sizing)

- Called from (representative examples):
  - do_pg_backup_start (src/backend/access/transam/xlog.c:8809)
  - do_pg_backup_stop (src/backend/access/transam/xlog.c:9136)
  - build_backup_content (src/backend/access/transam/xlogbackup.c:29)
  - pg_backup_start (src/backend/access/transam/xlogfuncs.c:92)
  - perform_base_backup (src/backend/backup/basebackup.c:241, 263)
  - PrepareForIncrementalBackup (src/backend/backup/basebackup_incremental.c:266)
  - SessionBackupState (src/include/access/xlog.h:291, 293)

## Notes and Other Information
- The structure supports both traditional full backups and incremental backup operations through dedicated fields (istartpoint, istarttli)
- Timeline consistency is maintained throughout the backup process by tracking both start and stop timeline IDs
- The started_in_recovery flag is crucial for understanding the backup context and may affect recovery procedures
- All WAL positions are stored as XLogRecPtr types, ensuring precise positioning within the transaction log
- The backup name field includes extra space for null-termination to prevent buffer overflow issues
- This structure is fundamental to PostgreSQL's backup and recovery infrastructure, enabling consistent point-in-time recovery operations