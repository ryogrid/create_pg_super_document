# reap_child

## Location
[src/bin/pg_upgrade/parallel.c:278-341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/parallel.c#L278-L341)

## Overview
Collects status from completed worker child processes or threads, managing the lifecycle of parallel jobs in PostgreSQL's pg_upgrade utility.

## Definition

```c
struct into the now-dead slot, and the
		 * now-dead slot to the end for reuse by the next thread. Though the
		 * thread struct is in use by another thread, we can safely swap the
		 * struct pointers within the array.
		 */
		tmp_args = cur_thread_args[thread_num];
```
## Detailed Description
This function is responsible for managing the lifecycle of parallel worker processes (Unix) or threads (Windows) in pg_upgrade. It collects exit status from completed workers, handles cleanup of system resources, and maintains the parallel job count. The function provides both blocking and non-blocking modes of operation.

On Unix systems, it uses waitpid() to collect child process exit status and handles process cleanup. On Windows, it uses WaitForMultipleObjects() to wait for thread completion, retrieves thread exit codes, and manages thread handle cleanup to prevent resource leaks.

The function includes sophisticated thread/process management on Windows, including array compaction to maintain efficient slot usage when workers complete out of order.

## Parameters / Member Variables
- : Boolean flag controlling blocking behavior
  - : Block until a child/thread completes (blocking mode)
  - : Return immediately if no completed children available (non-blocking mode)

## Dependencies
- Functions called/Symbols referenced:
  - [waitpid](../w/waitpid.md) (Unix)
  - WaitForMultipleObjects (Windows)
  - GetExitCodeThread (Windows)
  - CloseHandle (Windows)
  - [pg_fatal](../p/pg_fatal.md) (error reporting)
- Called from (representative examples):
  - [parallel_exec_prog](../p/parallel_exec_prog.md)
  - [parallel_transfer_all_new_dbs](../p/parallel_transfer_all_new_dbs.md)
  - [generate_old_dump](../g/generate_old_dump.md)
  - [create_new_objects](../c/create_new_objects.md)
  - [transfer_all_new_tablespaces](../t/transfer_all_new_tablespaces.md)

## Notes and Other Information
- Platform-specific implementation: Uses waitpid() on Unix and Windows threading APIs on Windows
- Resource management: Prevents handle leaks on Windows by properly closing thread handles
- Error handling: Validates exit status and reports abnormal terminations via pg_fatal()
- Job tracking: Maintains parallel_jobs counter to track active worker count
- Array management: On Windows, implements slot compaction to efficiently reuse thread argument structures
- Performance optimization: Supports both blocking and non-blocking operation modes for flexible job scheduling
- Thread safety: Carefully manages shared thread argument arrays on Windows platforms
- Return value: Returns true if a worker was reaped, false if no workers were available or completed

## Simplified Source

```c
bool
reap_child(bool wait_for_child)
{
    // Nothing to reap if no parallel jobs or single-threaded mode
    if (user_opts.jobs <= 1 || parallel_jobs == 0)
        return false;

#ifndef WIN32
    // Unix: use waitpid to collect child process status
    int work_status;
    pid_t child = waitpid(-1, &work_status, wait_for_child ? 0 : WNOHANG);

    if (child == (pid_t) -1)
        pg_fatal("%s() failed: %m", "waitpid");
    if (child == 0)
        return false;  // No children available or completed
    if (work_status != 0)
        pg_fatal("child process exited abnormally: status %d", work_status);
#else
    // Windows: wait for thread completion
    int thread_num = WaitForMultipleObjects(parallel_jobs, thread_handles,
                                           false, wait_for_child ? INFINITE : 0);

    if (thread_num == WAIT_TIMEOUT || thread_num == WAIT_FAILED)
        return false;

    thread_num -= WAIT_OBJECT_0;

    // Check thread exit code
    DWORD res;
    GetExitCodeThread(thread_handles[thread_num], &res);
    if (res != 0)
        pg_fatal("child worker exited abnormally: %m");

    // Clean up thread handle
    CloseHandle(thread_handles[thread_num]);

    // Compact arrays by moving last slot into dead child's position
    if (thread_num != parallel_jobs - 1)
    {
        thread_handles[thread_num] = thread_handles[parallel_jobs - 1];
        void *tmp_args = cur_thread_args[thread_num];
        cur_thread_args[thread_num] = cur_thread_args[parallel_jobs - 1];
        cur_thread_args[parallel_jobs - 1] = tmp_args;
    }
#endif

    // Decrement job count after cleanup
    parallel_jobs--;
    return true;
}
```