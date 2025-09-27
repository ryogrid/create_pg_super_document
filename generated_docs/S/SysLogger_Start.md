# SysLogger_Start

## Location
[src/backend/postmaster/syslogger.c:595-801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L595-L801)

## Overview
SysLogger_Start is a postmaster subroutine that initializes and launches the system logger subprocess, setting up the logging infrastructure including pipes, log files, and stderr redirection.

## Definition

```c
int
SysLogger_Start(void)
```
## Detailed Description
SysLogger_Start implements the initialization and startup sequence for PostgreSQL's logging collector process. This function is called by the postmaster to establish the logging infrastructure and create a dedicated logger process. Key responsibilities include:

1. **Pipe Creation**: Creates the communication pipe between postmaster/backends and the logger process (handles both Unix pipe() and Windows CreatePipe)
2. **Log Directory Setup**: Ensures the log directory exists and is writable
3. **Initial Log File Creation**: Opens the initial log files (stderr, CSV, JSON) to verify write permissions before launching the logger process
4. **Process Launch**: Forks/creates the syslogger child process using postmaster_child_launch
5. **Stderr Redirection**: Redirects the postmaster's stderr and stdout to the logging pipe for collection by the logger process
6. **Resource Cleanup**: Closes file handles in the postmaster after successful launch

The function handles platform differences between Unix/Linux and Windows, using different APIs for pipe creation and file descriptor management.

## Parameters / Member Variables
- Returns: Process ID of the launched syslogger process, or 0 on failure

## Dependencies
- Functions called/Symbols referenced:
  - pipe/CreatePipe (creates the logging pipe)
  - [MakePGDirectory](../M/MakePGDirectory.md) (ensures log directory exists)
  - [logfile_getname](../l/logfile_getname.md) (generates log file names)
  - [logfile_open](../l/logfile_open.md) (opens log files)
  - [syslogger_fdget](../s/syslogger_fdget.md) (gets file descriptors for EXEC_BACKEND)
  - [postmaster_child_launch](../p/postmaster_child_launch.md) (launches the logger process)
  - dup2 (redirects stderr/stdout to the pipe)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (initial startup)
  - [ServerLoop](ServerLoop.md) (during normal operation)
  - [process_pm_child_exit](../p/process_pm_child_exit.md) (when restarting failed logger)

## Notes and Other Information
- Only runs when Logging_collector is enabled in configuration
- Creates persistent pipes that survive logger process restarts
- Handles both regular stderr logging and structured logging (CSV, JSON formats)
- Implements robust error handling with FATAL errors for critical failures
- Uses different code paths for EXEC_BACKEND builds (Windows) vs fork-based systems
- The function ensures that log files are writable before launching the logger process to catch permission issues early
- After successful launch, the postmaster no longer writes directly to log files, instead sending all output through the pipe to the logger process

## Simplified Source

```c
// Simplified version of SysLogger_Start
int SysLogger_Start(void) {
    pid_t syslogger_pid;
    char *log_filename;

    // Core logic step 1: Check if logging is enabled
    if (!Logging_collector) {
        return 0;
    }

    // Core logic step 2: Create communication pipe (first time only)
    if (syslog_pipe_not_created) {
        create_pipe_for_logging();  // Unix: pipe(), Windows: CreatePipe()
    }

    // Core logic step 3: Ensure log directory exists
    create_log_directory_if_needed();

    // Core logic step 4: Create initial log files to verify permissions
    first_syslogger_file_time = time(NULL);

    // Open main stderr log file
    log_filename = logfile_getname(first_syslogger_file_time, NULL);
    syslogFile = logfile_open(log_filename, "a", false);

    // Open CSV log file if enabled
    if (csv_logging_enabled) {
        csv_filename = logfile_getname(first_syslogger_file_time, ".csv");
        csvlogFile = logfile_open(csv_filename, "a", false);
    }

    // Open JSON log file if enabled
    if (json_logging_enabled) {
        json_filename = logfile_getname(first_syslogger_file_time, ".json");
        jsonlogFile = logfile_open(json_filename, "a", false);
    }

    // Core logic step 5: Launch the syslogger child process
    syslogger_pid = postmaster_child_launch(B_LOGGER, startup_data, size, NULL);

    if (syslogger_pid == -1) {
        log_error("could not fork system logger");
        return 0;
    }

    // Core logic step 6: Redirect stderr/stdout to pipe (first time only)
    if (!redirection_done) {
        log_info("redirecting log output to logging collector process");

        // Redirect stdout and stderr to the pipe
        redirect_stdout_stderr_to_pipe();

        redirection_done = true;
    }

    // Core logic step 7: Clean up file handles in postmaster
    close_log_files_in_postmaster();

    return (int) syslogger_pid;
}
```

Key simplifications made:
- Removed detailed platform-specific pipe creation code
- Consolidated error handling to focus on main logic flow
- Abstracted low-level file descriptor operations
- Simplified the stderr redirection process
- Removed EXEC_BACKEND conditional compilation details
- Focused on the essential algorithm: check config → create pipe → create files → launch process → redirect → cleanup