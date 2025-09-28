# basebackup_read_file

## Location
[src/backend/backup/basebackup.c:2111-2131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L2111-L2131)

## Overview
Performs file read operations during base backup with proper error handling, wait event reporting, and optional validation of read completeness.

## Definition
```c
static ssize_t basebackup_read_file(int fd, char *buf, size_t nbytes, off_t offset, const char *filename, bool partial_read_ok)
```

## Detailed Description
This function provides a robust file reading interface specifically designed for base backup operations. It wraps the standard pg_pread function call with comprehensive error handling and monitoring capabilities. The function reports wait events to PostgreSQL's statistics system, enabling proper tracking of backup I/O operations. It includes validation to ensure complete reads when required, and provides detailed error messages that include the filename for better diagnostics during backup operations.

## Parameters / Member Variables
- `fd`: File descriptor of the file to read from
- `buf`: Buffer to store the read data
- `nbytes`: Number of bytes to read from the file
- `offset`: Offset position in the file to start reading from
- `filename`: Name of the file being read (used for error reporting)
- `partial_read_ok`: Boolean flag indicating whether partial reads are acceptable

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md) (with WAIT_EVENT_BASEBACKUP_READ)
  - [pg_pread](../p/pg_pread.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - ereport
- Called from (representative examples):
  - [read_file_data_into_buffer](../r/read_file_data_into_buffer.md)
  - [perform_base_backup](../p/perform_base_backup.md)

## Notes and Other Information
- Returns the number of bytes actually read from the file
- Static function used only within the basebackup.c module
- Implements comprehensive error reporting with filename context
- Uses PostgreSQL's wait event reporting system for monitoring
- Validates read completeness unless partial_read_ok is true
- Essential component for reliable file I/O during backup operations

## Simplified Source

```c
// Simplified version of basebackup_read_file
static ssize_t basebackup_read_file(int fd, char *buf, size_t nbytes, off_t offset,
                                   const char *filename, bool partial_read_ok) {
    ssize_t rc;

    // Report wait event and perform read
    pgstat_report_wait_start(WAIT_EVENT_BASEBACKUP_READ);
    rc = pg_pread(fd, buf, nbytes, offset);
    pgstat_report_wait_end();

    // Handle read errors
    if (rc < 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not read file \"%s\": %m", filename)));

    // Validate read completeness if required
    if (!partial_read_ok && rc > 0 && rc != nbytes)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not read file \"%s\": read %zd of %zu",
                        filename, rc, nbytes)));

    return rc;
}
```

Key simplifications made:
- Removed detailed comments while preserving core functionality
- Consolidated error handling logic
- Maintained wait event reporting for monitoring
- Preserved comprehensive error reporting with filename context
- Kept validation for read completeness when required