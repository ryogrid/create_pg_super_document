# parallel_exec_prog

## Location
[src/bin/pg_upgrade/parallel.c:62-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/parallel.c#L62-L152)

## Overview
Executes a command in parallel mode as part of PostgreSQL's pg_upgrade utility, providing concurrent execution capabilities for improved performance during database upgrades.

## Definition

```c
struct */
		pg_free(new_arg->log_file);
```
## Detailed Description
This function provides parallel execution capabilities for pg_upgrade operations. It has the same API as exec_prog but adds parallel execution support. The function manages a pool of worker processes (on Unix) or threads (on Windows) to execute commands concurrently, significantly improving performance during database upgrade operations.

When parallel jobs are disabled (user_opts.jobs <= 1), it falls back to sequential execution using exec_prog. In parallel mode, it manages process/thread lifecycle, including forking new processes on Unix systems or creating new threads on Windows, and handles job scheduling to respect the maximum number of concurrent jobs.

The function ensures proper stdio state before forking and includes comprehensive error handling for process/thread creation failures.

## Parameters / Member Variables
- : Path to the main log file where command output will be written
- : Optional path to an additional log file (can be NULL)
- : Printf-style format string for the command to execute
- : Variable arguments corresponding to the format string

## Dependencies
- Functions called/Symbols referenced:
  - [exec_prog](../e/exec_prog.md)
  - [reap_child](../r/reap_child.md)
  - [win32_exec_prog](../w/win32_exec_prog.md) (Windows only)
  - vsnprintf
  - [pg_malloc](pg_malloc.md)
  - [pg_malloc0](pg_malloc0.md)
  - [pg_free](pg_free.md)
  - [pg_strdup](pg_strdup.md)
  - fork (Unix)
  - _beginthreadex (Windows)
- Called from (representative examples):
  - [generate_old_dump](../g/generate_old_dump.md)
  - [create_new_objects](../c/create_new_objects.md)

## Notes and Other Information
- Platform-specific implementation: Uses fork() on Unix systems and _beginthreadex() on Windows
- Thread safety: On Windows, maintains thread-safe argument structures and handles
- Memory management: Carefully manages memory allocation for thread arguments on Windows
- Error handling: Must throw errors rather than return error status due to parallel execution nature
- Job control: Respects user_opts.jobs limit and implements job harvesting through reap_child()
- Performance optimization: Significantly improves pg_upgrade performance by allowing concurrent execution of multiple operations

## Simplified Source

```c
void
parallel_exec_prog(const char *log_file, const char *opt_log_file,
                   const char *fmt, ...)
{
    va_list args;
    char cmd[MAX_STRING];

    // Build command string from format and arguments
    va_start(args, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, args);
    va_end(args);

    // If not using parallel jobs, execute sequentially
    if (user_opts.jobs <= 1)
        exec_prog(log_file, opt_log_file, true, true, "%s", cmd);
    else
    {
        // Parallel execution mode
#ifdef WIN32
        // Initialize Windows thread infrastructure if needed
        if (thread_handles == NULL)
            thread_handles = pg_malloc(user_opts.jobs * sizeof(HANDLE));
        if (exec_thread_args == NULL)
        {
            exec_thread_args = pg_malloc(user_opts.jobs * sizeof(exec_thread_arg *));
            for (int i = 0; i < user_opts.jobs; i++)
                exec_thread_args[i] = pg_malloc0(sizeof(exec_thread_arg));
        }
        cur_thread_args = (void **) exec_thread_args;
#endif

        // Clean up any completed jobs
        while (reap_child(false))
            ;

        // Wait for slot if at job limit
        if (parallel_jobs >= user_opts.jobs)
            reap_child(true);

        parallel_jobs++;
        fflush(NULL);  // Ensure clean stdio state

#ifndef WIN32
        // Unix: fork child process
        pid_t child = fork();
        if (child == 0)
            _exit(!exec_prog(log_file, opt_log_file, true, true, "%s", cmd));
        else if (child < 0)
            pg_fatal("could not create worker process: %m");
#else
        // Windows: create thread
        exec_thread_arg *new_arg = exec_thread_args[parallel_jobs - 1];
        pg_free(new_arg->log_file);
        new_arg->log_file = pg_strdup(log_file);
        pg_free(new_arg->opt_log_file);
        new_arg->opt_log_file = opt_log_file ? pg_strdup(opt_log_file) : NULL;
        pg_free(new_arg->cmd);
        new_arg->cmd = pg_strdup(cmd);

        HANDLE child = (HANDLE) _beginthreadex(NULL, 0, (void *) win32_exec_prog,
                                               new_arg, 0, NULL);
        if (child == 0)
            pg_fatal("could not create worker thread: %m");
        thread_handles[parallel_jobs - 1] = child;
#endif
    }
}
```