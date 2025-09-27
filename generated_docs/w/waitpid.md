# waitpid

## Location
[src/backend/postmaster/postmaster.c:4594-4647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4594-L4647)

## Overview
A Windows-specific implementation of the POSIX waitpid() system call that provides a subset of functionality for checking child process status without blocking.

## Definition
```c
static pid_t waitpid(pid_t pid, int *exitstatus, int options)
```

## Detailed Description
This function provides a Windows-compatible implementation of the POSIX waitpid() system call. It assumes that pid is -1 (check all child processes) and options is WNOHANG (don't wait/block). The implementation uses Windows I/O completion ports to check for dead child processes by consuming win32_deadchild_waitinfo structures from a queue. When a dead child is found, it retrieves the exit code, cleans up Windows handles, and returns the process ID. If no dead children are available, it returns -1 with errno set to EAGAIN.

## Parameters / Member Variables
- `pid`: Process ID to wait for (assumed to be -1 for all children)
- `exitstatus`: Pointer to integer where the exit status will be stored
- `options`: Wait options (assumed to be WNOHANG for non-blocking behavior)

## Dependencies
- Functions called/Symbols referenced:
  - [win32_deadchild_waitinfo](win32_deadchild_waitinfo.md) (structure for Windows child process information)
  - GetQueuedCompletionStatus (Windows API for completion port)
  - UnregisterWaitEx (Windows API to remove wait handle)
  - GetExitCodeProcess (Windows API to get process exit code)
  - CloseHandle (Windows API to close process handle)
  - [write_stderr](write_stderr.md) (PostgreSQL error reporting function)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - EAGAIN (errno constant)
- Called from:
  - [process_pm_child_exit](../p/process_pm_child_exit.md) (src/backend/postmaster/postmaster.c:2364)
  - [BaseBackup](../B/BaseBackup.md) (src/bin/pg_basebackup/pg_basebackup.c:2242)
  - [wait_for_postmaster_start](wait_for_postmaster_start.md) (src/bin/pg_ctl/pg_ctl.c:664)
  - [reap_child](../r/reap_child.md) (src/bin/pg_upgrade/parallel.c:292)
  - [regression_main](../r/regression_main.md) (src/test/regress/pg_regress.c:2531)

## Notes and Other Information
- This is a Windows-only implementation, conditionally compiled for Windows platforms
- The function is static and only accessible within postmaster.c
- Implements a subset of POSIX waitpid() functionality, specifically for non-blocking checks of all child processes
- Uses Windows I/O completion ports for asynchronous child process monitoring
- Properly cleans up Windows handles and allocated memory structures
- Falls back to a fixed exit code (255) if the actual exit code cannot be retrieved
- Located in src/backend/postmaster/postmaster.c:4594-4647

## Simplified Source

```c
// Simplified version of waitpid
// Windows-specific implementation of POSIX waitpid() for non-blocking child process checking
static pid_t waitpid(pid_t pid, int *exitstatus, int options) {
    win32_deadchild_waitinfo *childinfo;
    DWORD exitcode;
    DWORD dwd;
    ULONG_PTR key;
    OVERLAPPED *ovl;

    // Try to get a dead child process from the completion queue
    if (!GetQueuedCompletionStatus(win32ChildQueue, &dwd, &key, &ovl, 0)) {
        errno = EAGAIN;  // No dead children available
        return -1;
    }

    // Extract child process information
    childinfo = (win32_deadchild_waitinfo *) key;
    pid = childinfo->procId;

    // Clean up the wait handle
    UnregisterWaitEx(childinfo->waitHandle, NULL);

    // Get the process exit code
    if (!GetExitCodeProcess(childinfo->procHandle, &exitcode)) {
        write_stderr("could not read exit code for process\n");
        exitcode = 255;  // Fallback exit code
    }
    *exitstatus = exitcode;

    // Clean up process handle and allocated memory
    CloseHandle(childinfo->procHandle);
    pfree(childinfo);

    return pid;  // Return the PID of the dead child
}
```

Key simplifications made:
- Removed detailed comments about Windows-specific behavior assumptions
- Consolidated error handling logic
- Focused on the main execution flow: check queue → extract info → cleanup → return
- Abstracted the Windows API complexity with brief descriptive comments
- Maintained the essential algorithm structure and return values