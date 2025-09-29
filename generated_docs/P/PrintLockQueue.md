# PrintLockQueue

## Location
[src/backend/storage/lmgr/deadlock.c:1050-1071](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/deadlock.c#L1050-L1071)

## Overview
PrintLockQueue is a debugging utility function that prints the current state of a lock's wait queue to standard output, showing the process IDs of all waiting processes.

## Definition
static void PrintLockQueue(LOCK *lock, const char *info)

## Detailed Description
This function provides debugging and diagnostic output for PostgreSQL's lock management system. It traverses the wait queue of a given lock and prints the process IDs of all processes currently waiting for that lock. The output includes a descriptive info string and the lock's memory address for identification purposes.

The function is primarily used during deadlock detection and resolution phases to provide visibility into the current state of lock queues. This information is valuable for debugging deadlock scenarios, understanding lock contention patterns, and verifying that deadlock resolution algorithms are working correctly.

The output format consists of the info string, the lock's memory address, followed by a space-separated list of process IDs currently waiting in the queue, terminated by a newline. The function ensures output is immediately flushed to guarantee visibility in debugging scenarios.

## Parameters / Member Variables
- `lock`: Pointer to the LOCK structure whose wait queue should be printed
- `info`: Descriptive string to be included in the output for context identification

## Dependencies
- Functions called/Symbols referenced:
  - dclist_foreach
  - dlist_container
  - printf
  - fflush
- Called from (representative examples):
  - [DeadLockCheck](../D/DeadLockCheck.md)

## Notes and Other Information
- Used exclusively for debugging and diagnostic purposes
- Output goes to stdout and is immediately flushed for real-time visibility
- Only compiled and active when debugging lock management or deadlock detection
- Provides essential visibility during complex deadlock resolution scenarios
- Format: "[info] lock [address] queue [pid1] [pid2] ...\n"
- Traverses the lock's wait queue in order, showing the current queue state
- Commonly used in DeadLockCheck function to show queue states before and after deadlock resolution attempts

## Simplified Source
```c
static void PrintLockQueue(LOCK *lock, const char *info) {
    // Get reference to the lock's wait queue
    dclist_head *waitQueue = &lock->waitProcs;

    // Print header with info and lock address
    printf("%s lock %p queue ", info, lock);

    // Iterate through all waiting processes
    dlist_iter proc_iter;
    dclist_foreach(proc_iter, waitQueue) {
        // Extract process from queue node
        PGPROC *proc = dlist_container(PGPROC, links, proc_iter.cur);

        // Print process ID
        printf(" %d", proc->pid);
    }

    // Complete the line and flush output for immediate visibility
    printf("\n");
    fflush(stdout);
}
```