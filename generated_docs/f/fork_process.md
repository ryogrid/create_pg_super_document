# fork_process

## Location
[src/backend/postmaster/fork_process.c:33-128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/fork_process.c#L33-L128)

## Overview
A wrapper function for the Unix fork() system call that safely handles signal masking and process-specific initialization for PostgreSQL child processes.

## Definition
```c
pid_t fork_process(void)
```

## Detailed Description
The `fork_process` function is a PostgreSQL-specific wrapper around the standard Unix `fork()` system call. It provides essential safety mechanisms and initialization procedures required for PostgreSQLs multi-process architecture. The function blocks signals during the fork operation to prevent race conditions, handles Linux-specific profiling timer preservation, manages Out-of-Memory (OOM) score adjustments for child processes, and initializes random number generation in the child process.

The function ensures that child processes start with a clean signal state and proper initialization, which is critical for PostgreSQLs postmaster-backend process model. It also includes Linux-specific optimizations for profiling and OOM protection.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - fork (standard Unix system call)
  - fflush
  - getitimer (Linux profiling support)
  - [setitimer](../s/setitimer.md) (Linux profiling support)
  - sigprocmask (signal handling)
  - getpid
  - getenv
  - open
  - write
  - close
  - strlen
  - [pg_strong_random_init](../p/pg_strong_random_init.md)
- Called from (representative examples):
  - [postmaster_child_launch](../p/postmaster_child_launch.md)
  - [internal_forkexec](../i/internal_forkexec.md)

## Notes and Other Information
- Return values match those of the standard fork() system call: -1 on failure, 0 in child process, child PID in parent process
- Signals are blocked during forking using BlockSig mask to prevent race conditions
- On Linux, includes special handling for profiling timers (LINUX_PROFILE compilation flag)
- Implements Linux-specific OOM (Out-of-Memory) score adjustment via PG_OOM_ADJUST_FILE and PG_OOM_ADJUST_VALUE environment variables
- Child processes must explicitly unblock signals after fork
- Initializes strong random number generation in child processes via pg_strong_random_init()
- Uses fflush(NULL) before forking to prevent double-output problems in stdio streams
- Located in src/backend/postmaster/fork_process.c:33-128

## Simplified Source

```c
// Simplified version of fork_process
pid_t fork_process(void) {
    pid_t result;
    sigset_t save_mask;

    // Flush stdio to avoid double-output problems
    fflush(NULL);

    // Block signals before forking to prevent race conditions
    sigprocmask(SIG_SETMASK, &BlockSig, &save_mask);

    // Perform the actual fork
    result = fork();

    if (result == 0) {
        // In child process: perform child-specific initialization

        // Update process ID variable
        MyProcPid = getpid();

        // Handle Linux OOM score adjustment if configured
        const char *oomfilename = getenv("PG_OOM_ADJUST_FILE");
        if (oomfilename != NULL) {
            int fd = open(oomfilename, O_WRONLY, 0);
            if (fd >= 0) {
                const char *oomvalue = getenv("PG_OOM_ADJUST_VALUE");
                if (oomvalue == NULL)
                    oomvalue = "0";
                write(fd, oomvalue, strlen(oomvalue));
                close(fd);
            }
        }

        // Initialize random number generation for child
        pg_strong_random_init();
    }
    else {
        // In parent process: restore original signal mask
        sigprocmask(SIG_SETMASK, &save_mask, NULL);
    }

    return result;
}
```

Key simplifications made:
- Removed Linux profiling timer handling (LINUX_PROFILE conditional code)
- Simplified OOM adjustment logic while preserving core functionality
- Removed detailed comments and consolidated error handling
- Focused on the main execution path: signal blocking, fork, child initialization, signal restoration
- Preserved all essential functionality: signal masking, fork operation, child PID update, OOM handling, and random initialization