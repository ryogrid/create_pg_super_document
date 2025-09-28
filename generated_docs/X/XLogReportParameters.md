# XLogReportParameters

## Location
[src/backend/access/transam/xlog.c:8119-8181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L8119-L8181)

## Overview
XLogReportParameters checks if critical GUC parameters for hot standby have changed and updates both the WAL and pg_control file to reflect these changes.

## Definition
```c
static void XLogReportParameters(void)
```

## Detailed Description
XLogReportParameters is responsible for monitoring changes to configuration parameters that are critical for hot standby and streaming replication functionality. When any of these parameters change, the function ensures consistency by writing the new values to both the WAL (via an XLOG_PARAMETER_CHANGE record) and the pg_control file. This synchronization is essential for hot standby servers to operate correctly, as they need to know the primary server's configuration parameters.

The function compares current parameter values against those stored in the ControlFile and takes action if differences are detected. For certain parameters like connection limits, WAL logging only occurs if WAL archiving is enabled or wal_level is not minimal, since these values are not relevant for minimal WAL configurations.

## Parameters / Member Variables
This function takes no parameters but monitors these critical GUC parameters:
- `wal_level`: Current WAL logging level setting
- `wal_log_hints`: Whether to log hint bits
- `MaxConnections`: Maximum number of concurrent connections
- `max_worker_processes`: Maximum background worker processes
- `max_wal_senders`: Maximum WAL sender processes for replication
- `max_prepared_xacts`: Maximum prepared transactions
- `max_locks_per_xact`: Maximum locks per transaction
- `track_commit_timestamp`: Whether to track commit timestamps

## Dependencies
- Functions called/Symbols referenced:
  - XLogIsNeeded
  - [XLogBeginInsert](XLogBeginInsert.md)
  - [XLogRegisterData](XLogRegisterData.md)
  - [XLogInsert](XLogInsert.md)
  - [XLogFlush](XLogFlush.md)
  - [UpdateControlFile](../U/UpdateControlFile.md)
  - [xl_parameter_change](../x/xl_parameter_change.md) (struct type)
  - XLOG_PARAMETER_CHANGE (record type)
- Called from (representative examples):
  - RefreshXLogWriteResult
  - [StartupXLOG](../S/StartupXLOG.md)

## Notes and Other Information
- This function is static and only called internally within the WAL subsystem
- Parameter changes are only WAL-logged if archiving is enabled or wal_level is not minimal
- The function updates both the WAL and pg_control file atomically under ControlFileLock
- Changes to backend slot numbers don't require WAL logging if archiving is disabled
- The xl_parameter_change record contains all monitored parameters to ensure hot standby servers can adjust accordingly
- This mechanism ensures that configuration changes on the primary are properly communicated to standby servers for consistent operation

## Simplified Source

```c
// Simplified version of XLogReportParameters
static void XLogReportParameters(void) {
    // Check if any critical hot standby parameters have changed
    if (wal_level != ControlFile->wal_level ||
        wal_log_hints != ControlFile->wal_log_hints ||
        MaxConnections != ControlFile->MaxConnections ||
        max_worker_processes != ControlFile->max_worker_processes ||
        max_wal_senders != ControlFile->max_wal_senders ||
        max_prepared_xacts != ControlFile->max_prepared_xacts ||
        max_locks_per_xact != ControlFile->max_locks_per_xact ||
        track_commit_timestamp != ControlFile->track_commit_timestamp) {

        // Write parameter change to WAL if logging is needed
        if (wal_level != ControlFile->wal_level || XLogIsNeeded()) {
            xl_parameter_change xlrec;

            // Populate the change record with current values
            xlrec.MaxConnections = MaxConnections;
            xlrec.max_worker_processes = max_worker_processes;
            xlrec.max_wal_senders = max_wal_senders;
            xlrec.max_prepared_xacts = max_prepared_xacts;
            xlrec.max_locks_per_xact = max_locks_per_xact;
            xlrec.wal_level = wal_level;
            xlrec.wal_log_hints = wal_log_hints;
            xlrec.track_commit_timestamp = track_commit_timestamp;

            // Insert the WAL record and flush
            XLogBeginInsert();
            XLogRegisterData((char *) &xlrec, sizeof(xlrec));
            XLogRecPtr recptr = XLogInsert(RM_XLOG_ID, XLOG_PARAMETER_CHANGE);
            XLogFlush(recptr);
        }

        // Update control file with new parameter values
        LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);

        ControlFile->MaxConnections = MaxConnections;
        ControlFile->max_worker_processes = max_worker_processes;
        ControlFile->max_wal_senders = max_wal_senders;
        ControlFile->max_prepared_xacts = max_prepared_xacts;
        ControlFile->max_locks_per_xact = max_locks_per_xact;
        ControlFile->wal_level = wal_level;
        ControlFile->wal_log_hints = wal_log_hints;
        ControlFile->track_commit_timestamp = track_commit_timestamp;

        UpdateControlFile();
        LWLockRelease(ControlFileLock);
    }
}
```

Key simplifications made:
- Removed detailed comments about archiving conditions for clarity
- Consolidated variable declaration with assignment in WAL logging section
- Added descriptive comments for each major logic block
- Maintained all essential logic while improving readability
- Preserved the dual update mechanism (WAL + control file)