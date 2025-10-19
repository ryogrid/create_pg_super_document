# parallel_transfer_all_new_dbs

## Location
[src/bin/pg_upgrade/parallel.c:172-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/parallel.c#L172-L262)

## Overview
Performs parallel transfer of all new databases during PostgreSQL upgrade, enabling concurrent tablespace transfers for improved performance.

## Definition

```c
struct */
		new_arg->old_db_arr = old_db_arr;
```
## Detailed Description
This function provides parallel execution capabilities for transferring all new databases during a PostgreSQL upgrade. It has the same API as transfer_all_new_dbs but adds parallel execution by transferring multiple tablespaces concurrently. The function manages worker processes (Unix) or threads (Windows) to handle database transfers in parallel, significantly improving upgrade performance for systems with multiple tablespaces.

When parallel jobs are disabled (user_opts.jobs <= 1), it falls back to sequential execution using transfer_all_new_dbs. In parallel mode, it handles the complete lifecycle of worker processes/threads, including proper job scheduling, resource management, and error handling.

The function ensures proper stdio state before forking and includes comprehensive error handling for process/thread creation failures.

## Parameters / Member Variables
- : Array of database information structures from the old cluster
- : Array of database information structures for the new cluster  
- : Path to the old PostgreSQL data directory
- : Path to the new PostgreSQL data directory
- : Path to the specific old tablespace being transferred (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [transfer_all_new_dbs](../t/transfer_all_new_dbs.md)
  - [reap_child](../r/reap_child.md)
  - [win32_transfer_all_new_dbs](../w/win32_transfer_all_new_dbs.md) (Windows only)
  - [pg_malloc](pg_malloc.md)
  - [pg_malloc0](pg_malloc0.md)
  - [pg_free](pg_free.md)
  - [pg_strdup](pg_strdup.md)
  - fork (Unix)
  - _beginthreadex (Windows)
- Called from (representative examples):
  - [transfer_all_new_tablespaces](../t/transfer_all_new_tablespaces.md)

## Notes and Other Information
- Platform-specific implementation: Uses fork() on Unix systems and _beginthreadex() on Windows
- Thread safety: On Windows, maintains thread-safe argument structures with proper memory management
- Memory management: Carefully manages allocation and deallocation of thread arguments on Windows
- Job control: Respects user_opts.jobs limit and implements job harvesting through reap_child()
- Performance optimization: Enables concurrent tablespace transfers, dramatically improving upgrade time for large databases
- Error handling: Uses pg_fatal() for critical errors that should terminate the upgrade process
- Process isolation: On Unix, uses _exit(0) to avoid atexit() functions in child processes

## Simplified Source

```c
void
parallel_transfer_all_new_dbs(DbInfoArr *old_db_arr, DbInfoArr *new_db_arr,
                              char *old_pgdata, char *new_pgdata,
                              char *old_tablespace)
{
    // If not using parallel jobs, execute sequentially
    if (user_opts.jobs <= 1)
        transfer_all_new_dbs(old_db_arr, new_db_arr, old_pgdata, new_pgdata, NULL);
    else
    {
        // Parallel execution mode
#ifdef WIN32
        // Initialize Windows thread infrastructure if needed
        if (thread_handles == NULL)
            thread_handles = pg_malloc(user_opts.jobs * sizeof(HANDLE));
        if (transfer_thread_args == NULL)
        {
            transfer_thread_args = pg_malloc(user_opts.jobs * sizeof(transfer_thread_arg *));
            for (int i = 0; i < user_opts.jobs; i++)
                transfer_thread_args[i] = pg_malloc0(sizeof(transfer_thread_arg));
        }
        cur_thread_args = (void **) transfer_thread_args;
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
        {
            transfer_all_new_dbs(old_db_arr, new_db_arr, old_pgdata, new_pgdata, old_tablespace);
            _exit(0);  // Clean exit from child
        }
        else if (child < 0)
            pg_fatal("could not create worker process: %m");
#else
        // Windows: create thread
        transfer_thread_arg *new_arg = transfer_thread_args[parallel_jobs - 1];
        new_arg->old_db_arr = old_db_arr;
        new_arg->new_db_arr = new_db_arr;
        pg_free(new_arg->old_pgdata);
        new_arg->old_pgdata = pg_strdup(old_pgdata);
        pg_free(new_arg->new_pgdata);
        new_arg->new_pgdata = pg_strdup(new_pgdata);
        pg_free(new_arg->old_tablespace);
        new_arg->old_tablespace = old_tablespace ? pg_strdup(old_tablespace) : NULL;

        HANDLE child = (HANDLE) _beginthreadex(NULL, 0, (void *) win32_transfer_all_new_dbs,
                                               new_arg, 0, NULL);
        if (child == 0)
            pg_fatal("could not create worker thread: %m");
        thread_handles[parallel_jobs - 1] = child;
#endif
    }
}
```