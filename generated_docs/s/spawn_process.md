# spawn_process

## Location
[src/test/regress/pg_regress.c:1199-1260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1199-L1260)

## Overview
Creates a child process to execute a shell command asynchronously, returning the process identifier for later monitoring without waiting for completion.

## Definition
```c
PID_TYPE spawn_process(const char *cmdline)
```

## Detailed Description
This function creates a new process to execute a shell command without blocking the parent process. It provides platform-specific implementations for Unix-like systems (using fork/exec) and Windows (using CreateProcess). The function is designed for parallel test execution, allowing multiple test processes to run concurrently.

On Unix systems, the function uses fork() to create a child process, then uses execl() to execute the shell command via the configured shell program. To optimize process management, it prefixes the command with "exec" to replace the shell process rather than creating a subprocess chain, reducing the total number of processes per parallel test.

On Windows, it uses CreateRestrictedProcess() to spawn the command via CMD.EXE (or the COMSPEC environment variable), properly formatting the command line and managing Windows process handles.

## Parameters / Member Variables
- `cmdline`: Shell command string to execute in the spawned process

## Dependencies
- Functions called/Symbols referenced:
  - fflush (Unix)
  - [pg_disable_aslr](../p/pg_disable_aslr.md) (Unix, if EXEC_BACKEND defined)
  - fork (Unix)
  - bail (Unix)
  - [psprintf](../p/psprintf.md) (Unix)
  - execl (Unix) 
  - bail_noatexit (Unix)
  - getenv (Windows)
  - memset (Windows)
  - [CreateRestrictedProcess](../C/CreateRestrictedProcess.md) (Windows)
  - CloseHandle (Windows)
  - shellprog (global variable, Unix)
- Called from (representative examples):
  - [regression_main](../r/regression_main.md)
  - [psql_start_test](../p/psql_start_test.md)
  - [ecpg_start_test](../e/ecpg_start_test.md)
  - [isolation_start_test](../i/isolation_start_test.md)

## Notes and Other Information
- Returns PID_TYPE which is pid_t on Unix systems and HANDLE on Windows
- On Unix, flushes all I/O buffers before forking to prevent duplicate output
- Uses pg_disable_aslr() on systems with EXEC_BACKEND to ensure consistent memory layout
- On Unix, uses bail_noatexit() in child process to avoid atexit handlers that could interfere with the parent
- On Windows, finds the command processor via COMSPEC environment variable, defaulting to "CMD"
- Windows implementation closes the thread handle immediately since only the process handle is needed for monitoring
- The function is specifically optimized for parallel test execution scenarios
- Process cleanup and waiting must be handled separately by the caller

## Simplified Source

```c
PID_TYPE spawn_process(const char *cmdline)
{
#ifndef WIN32
    pid_t pid;

    // Flush buffers before fork to prevent duplicate output
    fflush(NULL);

#ifdef EXEC_BACKEND
    pg_disable_aslr();
#endif

    pid = fork();
    if (pid == -1) {
        bail("could not fork: %m");
    }

    if (pid == 0) {
        // Child process: execute command via shell
        char *cmdline2 = psprintf("exec %s", cmdline);
        execl(shellprog, shellprog, "-c", cmdline2, (char *) NULL);
        bail_noatexit("could not exec \"%s\": %m", shellprog);
    }

    // Parent process: return child PID
    return pid;

#else
    // Windows implementation
    PROCESS_INFORMATION pi;
    char *cmdline2;
    const char *comspec = getenv("COMSPEC");

    if (comspec == NULL)
        comspec = "CMD";

    memset(&pi, 0, sizeof(pi));
    cmdline2 = psprintf("\"%s\" /c \"%s\"", comspec, cmdline);

    if (!CreateRestrictedProcess(cmdline2, &pi))
        exit(2);

    CloseHandle(pi.hThread);
    return pi.hProcess;
#endif
}
```