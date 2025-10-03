# SysLoggerMain

## Location
[src/backend/postmaster/syslogger.c:167-594](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L167-L594)

## Overview
SysLoggerMain is the main entry point and event loop for the PostgreSQL system logger process, responsible for collecting log output from all backend processes and managing log file rotation.

## Definition

```c
void
SysLoggerMain(char *startup_data, size_t startup_data_len)
```
## Detailed Description
SysLoggerMain implements the core functionality of the PostgreSQL logging system's dedicated logger process. This function serves as the main event loop that:

1. **Initializes the logger process environment**: Sets up file descriptors, signal handlers, and process identity
2. **Manages log file operations**: Handles opening, writing to, and rotating log files (stderr, CSV, and JSON formats)
3. **Processes incoming log data**: Reads log messages from other processes via pipes and writes them to appropriate log files
4. **Handles configuration changes**: Responds to SIGHUP signals to reload configuration and adjust logging behavior
5. **Manages log rotation**: Implements both time-based and size-based log rotation policies
6. **Coordinates process lifecycle**: Continues running until all other PostgreSQL processes have terminated (detected by EOF on the log pipe)

The function operates differently on Windows vs Unix-like systems, using threads on Windows and direct pipe reading on Unix systems.

## Parameters / Member Variables
- `*startup_data`: Serialized startup data containing file descriptors and configuration (used only in EXEC_BACKEND builds)
- `startup_data_len`: Length of the startup_data buffer, expected to be sizeof(SysloggerStartupData) in EXEC_BACKEND builds or 0 otherwise
## Dependencies
- Functions called/Symbols referenced:
  - [syslogger_fdopen](../s/syslogger_fdopen.md) (re-opens log files from passed descriptors)
  - [process_pipe_input](../p/process_pipe_input.md) (processes incoming log data)
  - [logfile_rotate](../l/logfile_rotate.md) (handles log file rotation)
  - [set_next_rotation_time](../s/set_next_rotation_time.md) (calculates next rotation time)
  - [update_metainfo_datafile](../u/update_metainfo_datafile.md) (updates metadata file)
  - [CreateWaitEventSet](../C/CreateWaitEventSet.md)/WaitEventSetWait (event loop management)
  - [SignalHandlerForConfigReload](SignalHandlerForConfigReload.md)/sigUsr1Handler (signal handling)
- Called from (representative examples):
  - child_process_kind (process launch infrastructure)
  - Referenced in syslogger.h (header declarations)

## Notes and Other Information
- This is a long-running process that typically outlives all other PostgreSQL processes
- Uses a sophisticated event-driven architecture with WaitEventSet for efficient I/O multiplexing
- Supports multiple log formats simultaneously (stderr, CSV, JSON)
- Implements robust error handling to ensure log data is not lost even during system shutdown
- On Windows, uses a separate thread (pipeThread) for data transfer due to platform limitations
- The process ignores most termination signals and only exits when it detects that all other processes have terminated (pipe EOF)
- Includes extensive configuration reload handling to dynamically adjust logging behavior without restart

## Simplified Source

```c
// Simplified version of SysLoggerMain
void SysLoggerMain(char *startup_data, size_t startup_data_len) {
    char logbuffer[READ_BUF_SIZE];
    int bytes_in_logbuffer = 0;
    char *currentLogDir;
    char *currentLogFilename;
    int currentLogRotationAge;
    pg_time_t now;
    WaitEventSet *wes;

    // Step 1: Initialize log files from startup data
    #ifdef EXEC_BACKEND
    SysloggerStartupData *slsdata = (SysloggerStartupData *) startup_data;
    syslogFile = syslogger_fdopen(slsdata->syslogFile);
    csvlogFile = syslogger_fdopen(slsdata->csvlogFile);
    jsonlogFile = syslogger_fdopen(slsdata->jsonlogFile);
    #endif

    // Step 2: Clean up postmaster context and initialize process
    if (PostmasterContext) {
        MemoryContextDelete(PostmasterContext);
        PostmasterContext = NULL;
    }

    MyBackendType = B_LOGGER;
    init_ps_display(NULL);

    // Step 3: Redirect stderr to /dev/null if restarted
    if (redirection_done) {
        int fd = open(DEVNULL, O_WRONLY, 0);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        if (fd != -1) {
            dup2(fd, STDOUT_FILENO);
            dup2(fd, STDERR_FILENO);
            close(fd);
        }
    }

    // Step 4: Close write end of pipe for EOF detection
    close(syslogPipe[1]);
    syslogPipe[1] = -1;

    // Step 5: Set up signal handlers
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGUSR1, sigUsr1Handler);  // log rotation request
    // Ignore termination signals - exit only on pipe EOF
    pqsignal(SIGINT, SIG_IGN);
    pqsignal(SIGTERM, SIG_IGN);
    pqsignal(SIGQUIT, SIG_IGN);

    sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

    // Step 6: Initialize log file names and rotation settings
    last_sys_file_name = logfile_getname(first_syslogger_file_time, NULL);
    if (csvlogFile != NULL)
        last_csv_file_name = logfile_getname(first_syslogger_file_time, ".csv");
    if (jsonlogFile != NULL)
        last_json_file_name = logfile_getname(first_syslogger_file_time, ".json");

    currentLogDir = pstrdup(Log_directory);
    currentLogFilename = pstrdup(Log_filename);
    currentLogRotationAge = Log_RotationAge;
    set_next_rotation_time();
    update_metainfo_datafile();

    whereToSendOutput = DestNone;

    // Step 7: Set up event waiting infrastructure
    wes = CreateWaitEventSet(NULL, 2);
    AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
    AddWaitEventToSet(wes, WL_SOCKET_READABLE, syslogPipe[0], NULL, NULL);

    // Step 8: Main event loop
    for (;;) {
        bool time_based_rotation = false;
        int size_rotation_for = 0;
        long cur_timeout;
        WaitEvent event;

        ResetLatch(MyLatch);

        // Handle configuration reload requests
        if (ConfigReloadPending) {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);

            // Check for directory/filename changes requiring rotation
            if (strcmp(Log_directory, currentLogDir) != 0) {
                pfree(currentLogDir);
                currentLogDir = pstrdup(Log_directory);
                rotation_requested = true;
                MakePGDirectory(Log_directory);
            }
            if (strcmp(Log_filename, currentLogFilename) != 0) {
                pfree(currentLogFilename);
                currentLogFilename = pstrdup(Log_filename);
                rotation_requested = true;
            }

            // Check for log format changes requiring rotation
            if (((Log_destination & LOG_DESTINATION_CSVLOG) != 0) != (csvlogFile != NULL))
                rotation_requested = true;
            if (((Log_destination & LOG_DESTINATION_JSONLOG) != 0) != (jsonlogFile != NULL))
                rotation_requested = true;

            // Handle rotation age changes
            if (currentLogRotationAge != Log_RotationAge) {
                currentLogRotationAge = Log_RotationAge;
                set_next_rotation_time();
            }

            if (rotation_disabled) {
                rotation_disabled = false;
                rotation_requested = true;
            }

            update_metainfo_datafile();
        }

        // Check for time-based rotation
        if (Log_RotationAge > 0 && !rotation_disabled) {
            now = (pg_time_t) time(NULL);
            if (now >= next_rotation_time)
                rotation_requested = time_based_rotation = true;
        }

        // Check for size-based rotation
        if (!rotation_requested && Log_RotationSize > 0 && !rotation_disabled) {
            if (ftell(syslogFile) >= Log_RotationSize * 1024L) {
                rotation_requested = true;
                size_rotation_for |= LOG_DESTINATION_STDERR;
            }
            if (csvlogFile != NULL && ftell(csvlogFile) >= Log_RotationSize * 1024L) {
                rotation_requested = true;
                size_rotation_for |= LOG_DESTINATION_CSVLOG;
            }
            if (jsonlogFile != NULL && ftell(jsonlogFile) >= Log_RotationSize * 1024L) {
                rotation_requested = true;
                size_rotation_for |= LOG_DESTINATION_JSONLOG;
            }
        }

        // Perform rotation if requested
        if (rotation_requested) {
            if (!time_based_rotation && size_rotation_for == 0)
                size_rotation_for = LOG_DESTINATION_STDERR | LOG_DESTINATION_CSVLOG | LOG_DESTINATION_JSONLOG;
            logfile_rotate(time_based_rotation, size_rotation_for);
        }

        // Calculate timeout for next rotation
        if (Log_RotationAge > 0 && !rotation_disabled) {
            pg_time_t delay = next_rotation_time - now;
            if (delay > 0) {
                if (delay > INT_MAX / 1000)
                    delay = INT_MAX / 1000;
                cur_timeout = delay * 1000L;
            } else {
                cur_timeout = 0;
            }
        } else {
            cur_timeout = -1L;
        }

        // Wait for events
        int rc = WaitEventSetWait(wes, cur_timeout, &event, 1, WAIT_EVENT_SYSLOGGER_MAIN);

        // Handle pipe data
        if (rc == 1 && event.events == WL_SOCKET_READABLE) {
            int bytesRead = read(syslogPipe[0], logbuffer + bytes_in_logbuffer,
                               sizeof(logbuffer) - bytes_in_logbuffer);

            if (bytesRead > 0) {
                bytes_in_logbuffer += bytesRead;
                process_pipe_input(logbuffer, &bytes_in_logbuffer);
                continue;
            } else if (bytesRead == 0) {
                // EOF: all processes have shut down
                pipe_eof_seen = true;
                flush_pipe_input(logbuffer, &bytes_in_logbuffer);
            }
        }

        // Exit when pipe EOF is detected
        if (pipe_eof_seen) {
            ereport(DEBUG1, (errmsg_internal("logger shutting down")));
            proc_exit(0);
        }
    }
}
```

Key simplifications made:
- Removed Windows-specific thread handling and critical sections
- Consolidated error handling to focus on main logic paths
- Abstracted platform-specific file descriptor operations
- Simplified signal setup by grouping related signals
- Removed detailed error checking for non-critical operations
- Focused on the core event loop structure and rotation logic
- Eliminated redundant comments and detailed error recovery paths
- Streamlined the configuration reload logic while preserving all functionality