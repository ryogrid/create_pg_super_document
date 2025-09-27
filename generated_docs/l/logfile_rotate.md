# logfile_rotate

## Location
[src/backend/postmaster/syslogger.c:1362-1410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L1362-L1410)

## Overview
Performs log file rotation for PostgreSQL's system logger, handling time-based and size-based rotation across multiple log destinations (stderr, CSV, and JSON logs).

## Definition

```c
static void
logfile_rotate(bool time_based_rotation, int size_rotation_for)
```
## Detailed Description
The  function is responsible for coordinating log file rotation across all active log destinations in PostgreSQL's system logger. It supports both time-based rotation (triggered by scheduled intervals) and size-based rotation (triggered when log files exceed size limits). The function ensures consistent naming across all log file types by using a unified timestamp, and handles the rotation process atomically by updating metadata and scheduling the next rotation time only after all individual destination rotations succeed.

## Parameters / Member Variables
- : Boolean flag indicating whether this rotation is triggered by time (true) or size (false)
- : When size-based rotation is performed, indicates which log destination triggered the rotation

## Dependencies
- Functions called/Symbols referenced:
  -  - Performs rotation for individual log destinations
  -  - Updates metadata file with current log file information
  -  - Schedules the next time-based rotation
  - , ,  - Log destination constants
  -  - PostgreSQL time type
- Called from (representative examples):
  -  - Main system logger process loop

## Notes and Other Information
- The function resets the  flag at the start to prevent duplicate rotations
- For time-based rotations, uses  rather than current time to prevent filename "slippage"
- Rotation is atomic - if any destination rotation fails, the entire process is aborted without updating metadata or scheduling next rotation
- Handles three log destinations: stderr (standard logs), CSV logs, and JSON logs
- The function maintains separate filename tracking variables for each log type (, , )

## Simplified Source

```c
// Simplified version of logfile_rotate
static void logfile_rotate(bool time_based_rotation, int size_rotation_for) {
    pg_time_t fntime;

    // Clear the rotation request flag
    rotation_requested = false;

    // Determine timestamp for new log files
    if (time_based_rotation) {
        // Use planned rotation time to avoid filename drift
        fntime = next_rotation_time;
    } else {
        // Use current time for size-based rotations
        fntime = time(NULL);
    }

    // Rotate stderr log files
    if (!logfile_rotate_dest(time_based_rotation, size_rotation_for, fntime,
                            LOG_DESTINATION_STDERR, &last_sys_file_name, &syslogFile)) {
        return;  // Abort on failure
    }

    // Rotate CSV log files
    if (!logfile_rotate_dest(time_based_rotation, size_rotation_for, fntime,
                            LOG_DESTINATION_CSVLOG, &last_csv_file_name, &csvlogFile)) {
        return;  // Abort on failure
    }

    // Rotate JSON log files
    if (!logfile_rotate_dest(time_based_rotation, size_rotation_for, fntime,
                            LOG_DESTINATION_JSONLOG, &last_json_file_name, &jsonlogFile)) {
        return;  // Abort on failure
    }

    // Update metadata file with new log file information
    update_metainfo_datafile();

    // Schedule next time-based rotation
    set_next_rotation_time();
}
```

Key simplifications made:
- Added explanatory comments for each major step
- Clarified the atomic nature of the operation (early returns on failure)
- Emphasized the dual rotation modes (time-based vs size-based)
- Highlighted the consistent timestamp usage across all destinations
- Simplified variable declarations and flow description