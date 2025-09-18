# LogChildExit

## Location
src/backend/postmaster/postmaster.c: 3062 - 3127

## Overview
LogChildExit provides detailed logging of child process termination events in PostgreSQL, including exit codes, signals, and the last known activity of crashed processes.

## Definition
static void LogChildExit(int lev, const char *procname, int pid, int exitstatus)

## Detailed Description
LogChildExit is a utility function that generates comprehensive log messages when PostgreSQL child processes terminate. The function analyzes the exit status to determine how the process died and formats appropriate log messages with different levels of detail. For abnormal exits (non-zero status), it attempts to retrieve and include the last known activity of the crashed backend process using pgstat_get_crashed_backend_activity. The function handles three types of process termination: normal exit (WIFEXITED), signal termination (WIFSIGNALED), and unrecognized status. For signal termination, it provides platform-specific formatting - on Windows it reports exception codes with a reference to ntstatus.h, while on Unix-like systems it reports signal numbers and names. The activity information helps administrators understand what the process was doing when it failed, making debugging easier.

## Parameters / Member Variables
- : Log level for the message (e.g., DEBUG2, LOG, ERROR)
- : Human-readable name of the process type (e.g., "server process", "background worker")
- : Process ID of the terminated child process
- : Raw exit status from the wait() system call, containing exit code or signal information

## Dependencies
- Functions called/Symbols referenced:
  - EXIT_STATUS_0
  - pgstat_get_crashed_backend_activity
  - WIFEXITED
  - WEXITSTATUS
  - WIFSIGNALED
  - WTERMSIG
  - pg_strsignal
  - ereport
  - errmsg
  - errdetail
  - errhint
- Called from (representative examples):
  - process_pm_child_exit
  - CleanupBackend
  - CleanupBackgroundWorker
  - HandleChildCrash

## Notes and Other Information
- Uses a 1024-byte buffer for activity information, matching the default track_activity_query_size
- Only retrieves crashed backend activity for abnormal exits to avoid unnecessary overhead
- Provides platform-specific error reporting for signal termination (Windows vs Unix)
- Includes translator comments for internationalization support
- The activity detail helps administrators debug process crashes by showing what SQL or operation was running
- Critical for PostgreSQL's observability and debugging capabilities during process failures