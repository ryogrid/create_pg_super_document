# set_ps_display

## Location
src/include/utils/ps_status.h: 40 - 47

## Overview
A static inline function that provides a convenient wrapper for updating the PostgreSQL process status display by calculating the string length at compile time when string constants are passed.

## Definition

```c
static inline void
set_ps_display(const char *activity)
```
## Detailed Description
The `set_ps_display` function is defined in `src/include/utils/ps_status.h` as a static inline wrapper around `set_ps_display_with_len`. This design allows the `strlen()` call to be evaluated at compilation time when string literals are passed as arguments, providing better performance than runtime string length calculation.

The function serves as the primary interface for PostgreSQL processes to update their process status display, which is visible in system process lists (like `ps` output on Unix systems). This is particularly useful for monitoring and debugging PostgreSQL operations, as different backend processes can display what operation they are currently performing.

The inline nature ensures there's no function call overhead while maintaining a clean, simple API for the most common use case of passing string constants to update the process title.

## Parameters / Member Variables
- `activity`: A null-terminated string describing the current activity or operation being performed by the process. This text will appear in the process status display.

## Dependencies
- Functions called/Symbols referenced:
  - set_ps_display_with_len
  - strlen (standard C library function)

- Called from (representative examples):
  - StartupXLOG (src/backend/access/transam/xlog.c:5822)
  - update_checkpoint_display (src/backend/access/transam/xlog.c:6814, 6823)
  - XLogFileRead (src/backend/access/transam/xlogrecovery.c:4208, 4250)
  - SendBaseBackup (src/backend/backup/basebackup.c:1009)
  - ProcessIncomingNotify (src/backend/commands/async.c:2197, 2223)
  - AutoVacWorkerMain (src/backend/postmaster/autovacuum.c:1563)
  - pgarch_archiveXlog (src/backend/postmaster/pgarch.c:528, 615)
  - WalRcvWaitForStartPosition (src/backend/replication/walreceiver.c:685, 736)
  - XLogWalRcvFlush (src/backend/replication/walreceiver.c:1027)
  - exec_replication_command (multiple locations in src/backend/replication/walsender.c)
  - XLogSendPhysical (src/backend/replication/walsender.c:3402)
  - BackendInitialize (src/backend/tcop/backend_startup.c:352)
  - PostgresMain (multiple locations in src/backend/tcop/postgres.c)
  - PerformAuthentication (src/backend/utils/init/postinit.c:245, 303)

## Notes and Other Information
- The function is implemented as a static inline to optimize performance, particularly when string literals are passed as arguments
- This is the preferred interface for most PostgreSQL code that needs to update process status, as it automatically handles string length calculation
- The underlying `set_ps_display_with_len` function performs the actual work of updating the process status buffer and flushing it to the system
- Process status updates are used extensively throughout PostgreSQL for monitoring different phases of operations like startup, checkpointing, WAL processing, replication, and client request handling
- The function is widely used across the PostgreSQL codebase, with calls in critical subsystems including transaction log processing, replication, autovacuum, archiving, and client connection handling