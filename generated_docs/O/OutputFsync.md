# OutputFsync

## Location
[src/bin/pg_basebackup/pg_recvlogical.c:185-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_recvlogical.c#L185-L212)

## Overview
The OutputFsync function performs synchronized writes to disk for logical replication output files, ensuring data durability at configurable intervals.

## Definition

```c
static bool
OutputFsync(TimestampTz now)
```
## Detailed Description
This function handles the fsync operation for logical replication output in pg_recvlogical. It updates the fsync timestamp and LSN tracking variables, then conditionally performs an actual fsync() system call to ensure that buffered data is written to persistent storage. The function implements several checks to determine when fsync is necessary:

1. Respects the fsync_interval setting (if <= 0, fsync is disabled)
2. Checks if an fsync is actually needed (output_needs_fsync flag)
3. Verifies that the output is a regular file (cannot fsync pipes, sockets, etc.)
4. Updates the output_fsync_lsn to match the current output_written_lsn

The function is crucial for ensuring data durability in logical replication scenarios where the client must guarantee that received logical changes have been safely persisted to disk before confirming receipt to the server.

## Parameters / Member Variables
- `now`: Current timestamp used to update the last fsync time tracking
## Dependencies
- Functions called/Symbols referenced:
  - fsync (system call to synchronize file data with storage device)
  - [pg_fatal](../p/pg_fatal.md) (error reporting function, called on fsync failure)
- Called from (representative examples):
  - [StreamLogicalLog](../S/StreamLogicalLog.md) (main logical replication streaming loop, multiple call sites)
  - [flushAndSendFeedback](../f/flushAndSendFeedback.md) (periodic flush and feedback operation)

## Notes and Other Information
- Static function only accessible within pg_recvlogical.c
- Uses global variables for state tracking (output_last_fsync, output_fsync_lsn, output_needs_fsync, etc.)
- Returns true on success or when fsync is not needed/possible
- Will terminate the program (via pg_fatal) if fsync fails, as this indicates a serious I/O problem
- Part of the durability guarantees required for logical replication clients
- Fsync frequency can be controlled via command-line options to balance performance vs. durability
- The function intelligently skips fsync for non-file outputs (stdout, pipes, etc.) where fsync is not applicable

## Simplified Source

```c
static bool
OutputFsync(TimestampTz now)
{
    // Update fsync tracking timestamps and LSN
    output_last_fsync = now;
    output_fsync_lsn = output_written_lsn;

    // Skip fsync if disabled, not needed, or not a regular file
    if (fsync_interval <= 0 || !output_needs_fsync || !output_isfile)
        return true;

    // Reset fsync flag and perform actual sync
    output_needs_fsync = false;
    if (fsync(outfd) != 0)
        pg_fatal("could not fsync file \"%s\": %m", outfile);

    return true;
}
```