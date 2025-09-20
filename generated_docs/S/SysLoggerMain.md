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
- : Serialized startup data containing file descriptors and configuration (used only in EXEC_BACKEND builds)
- : Length of the startup_data buffer, expected to be sizeof(SysloggerStartupData) in EXEC_BACKEND builds or 0 otherwise

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