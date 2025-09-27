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

## Simplified Source

```c
// Simplified version of CreateLockFile
static void CreateLockFile(const char *filename, bool amPostmaster,
                          const char *socketDir, bool isDDLock, const char *refName) {
    int fd;
    char buffer[MAXPGPATH * 2 + 256];
    int ntries;
    pid_t my_pid, my_p_pid, my_gp_pid;
    pid_t other_pid;
    int encoded_pid;

    // Get current process info for stale lockfile detection
    my_pid = getpid();
    my_p_pid = getppid();

    // Check for grandparent PID from environment (for pg_ctl)
    const char *envvar = getenv("PG_GRANDPARENT_PID");
    my_gp_pid = envvar ? atoi(envvar) : 0;

    // Retry loop to handle race conditions (max 100 attempts)
    for (ntries = 0; ; ntries++) {
        // Try atomic lockfile creation
        fd = open(filename, O_RDWR | O_CREAT | O_EXCL, pg_file_create_mode);
        if (fd >= 0)
            break;  // Success - exit retry loop

        // Handle creation failure
        if ((errno != EEXIST && errno != EACCES) || ntries > 100)
            ereport(FATAL, "could not create lock file");

        // Read existing lockfile to check if process is still alive
        fd = open(filename, O_RDONLY, pg_file_create_mode);
        if (fd < 0) {
            if (errno == ENOENT)
                continue;  // Race condition - file disappeared, retry
            ereport(FATAL, "could not open existing lock file");
        }

        // Read PID from existing lockfile
        int len = read(fd, buffer, sizeof(buffer) - 1);
        close(fd);

        if (len <= 0)
            ereport(FATAL, "lock file is empty or unreadable");

        buffer[len] = '\0';
        encoded_pid = atoi(buffer);
        other_pid = (encoded_pid < 0) ? -encoded_pid : encoded_pid;

        // Check if the PID in lockfile is still running
        if (other_pid != my_pid && other_pid != my_p_pid && other_pid != my_gp_pid) {
            if (kill(other_pid, 0) == 0 || (errno != ESRCH && errno != EPERM)) {
                // Process is still alive - cannot proceed
                ereport(FATAL, "lock file already exists, another server running");
            }
        }

        // For data directory locks, check shared memory segments
        if (isDDLock) {
            unsigned long shmem_id1, shmem_id2;
            if (parse_shmem_ids_from_buffer(buffer, &shmem_id1, &shmem_id2)) {
                if (PGSharedMemoryIsInUse(shmem_id1, shmem_id2))
                    ereport(FATAL, "shared memory still in use");
            }
        }

        // Stale lockfile detected - remove it and retry
        if (unlink(filename) < 0)
            ereport(FATAL, "could not remove old lock file");
    }

    // Write lockfile contents (PID, data dir, start time, port, socket dir)
    snprintf(buffer, sizeof(buffer), "%d\n%s\n" INT64_FORMAT "\n%d\n%s\n",
             amPostmaster ? (int)my_pid : -((int)my_pid),
             DataDir, MyStartTime, PostPortNumber, socketDir);

    // For standalone backends, add empty listen address line
    if (isDDLock && !amPostmaster)
        strlcat(buffer, "\n", sizeof(buffer));

    // Write and sync lockfile to disk
    if (write(fd, buffer, strlen(buffer)) != strlen(buffer)) {
        close(fd);
        unlink(filename);
        ereport(FATAL, "could not write lock file");
    }

    if (pg_fsync(fd) != 0) {
        close(fd);
        unlink(filename);
        ereport(FATAL, "could not sync lock file");
    }

    close(fd);

    // Register for automatic cleanup on process exit
    if (lock_files == NIL)
        on_proc_exit(UnlinkLockFiles, 0);

    lock_files = lcons(pstrdup(filename), lock_files);
}
```

Key simplifications made:
- Removed detailed error handling for clarity while keeping essential checks
- Abstracted shared memory parsing into conceptual `parse_shmem_ids_from_buffer()` function
- Consolidated similar error conditions into single ereport calls
- Simplified complex conditional expressions for readability
- Focused on the main execution path while preserving critical logic
- Removed platform-specific details (Windows vs Unix) for clarity
- Kept the essential race condition handling and atomic file creation logic