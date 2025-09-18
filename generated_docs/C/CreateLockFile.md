# CreateLockFile

## Location
[src/backend/utils/init/miscinit.c:1205-1509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1205-L1509)

## Overview
Creates PostgreSQL lockfiles (data directory or Unix socket lockfiles) with atomic file creation, stale process detection, and automatic cleanup registration.

## Definition
```c
static void CreateLockFile(const char *filename, bool amPostmaster, const char *socketDir, bool isDDLock, const char *refName)
```

## Detailed Description
This function implements PostgreSQL's lockfile creation mechanism, which prevents multiple PostgreSQL instances from using the same data directory or Unix socket file. It uses atomic file creation with O_EXCL to avoid race conditions, and includes sophisticated logic to detect and handle stale lockfiles from crashed processes. The function verifies that any existing lockfile doesn't belong to a currently running PostgreSQL process by checking PIDs and shared memory segments. On successful creation, it writes essential server information to the lockfile and registers an exit callback to ensure cleanup during shutdown.

## Parameters / Member Variables
- `filename`: Path to the lockfile to create
- `amPostmaster`: True if running as postmaster, false for standalone backend (affects PID encoding)
- `socketDir`: Unix socket directory path to include in lockfile contents
- `isDDLock`: True for data directory lockfiles, false for socket lockfiles (affects validation logic)
- `refName`: Reference name for error messages (data directory path or socket file path)

## Dependencies
- Functions called/Symbols referenced:
  - File operations: `open`, `close`, `read`, `write`, `unlink`, `pg_fsync`
  - Process operations: `getpid`, `getppid`, `kill`, `getenv`, `atoi`
  - PostgreSQL functions: `pgstat_report_wait_start/end`, `PGSharedMemoryIsInUse`
  - Memory/string operations: `snprintf`, `strlcat`, `strlen`, `strchr`, `sscanf`, `pstrdup`
  - [List](../L/List.md) operations: `lcons`, `NIL`
  - Callback registration: `on_proc_exit`, `UnlinkLockFiles`
  - Error handling: `ereport`, `errcode_for_file_access`, `errmsg`, `errhint`
  - Constants: `LOCK_FILE_LINE_SHMEM_KEY`, `INT64_FORMAT`, `MAXPGPATH`
  - Global variables: `DataDir`, `MyStartTime`, `PostPortNumber`, `lock_files`
- Called from (representative examples):
  - [CreateDataDirLockFile](CreateDataDirLockFile.md) - Creates data directory lockfile ($DATADIR/postmaster.pid)  
  - [CreateSocketLockFile](CreateSocketLockFile.md) - Creates Unix socket lockfile ($SOCKFILE.lock)

## Notes and Other Information
- Function is declared static, making it internal to miscinit.c compilation unit
- Uses O_EXCL flag for atomic lockfile creation to prevent race conditions
- Implements retry loop (up to 100 attempts) to handle transient conditions
- PIDs are encoded as negative values for standalone backends vs positive for postmaster
- Includes sophisticated stale lockfile detection via PID validation and shared memory checks
- Handles ancestry PID detection (parent/grandparent) via PG_GRANDPARENT_PID environment variable
- For data directory locks, validates shared memory segments are not still in use
- Writes standardized lockfile format with PID, data directory, start time, port, and socket directory
- Registers UnlinkLockFiles() as proc_exit callback for automatic cleanup
- Uses reverse-order unlinking (lcons) to ensure proper cleanup sequence
- Includes comprehensive error handling with context-appropriate error messages
- Reports wait events for performance monitoring during file operations
- File permissions use pg_file_create_mode for security (typically 0600/0640)