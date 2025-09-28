# s_lock_stuck

## Location
[src/backend/storage/lmgr/s_lock.c:80-98](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/s_lock.c#L80-L98)

## Overview
s_lock_stuck is a static function that reports and handles situations where a spinlock has become stuck (unable to be acquired after excessive waiting).

## Definition

```c
static void
s_lock_stuck(const char *file, int line, const char *func)
```
## Detailed Description
This function is called when a spinlock acquisition has been retried too many times without success, indicating a potential deadlock or system issue. The function behavior depends on compilation flags:

- In test mode (S_LOCK_TEST defined): Prints an error message to stderr and exits the program immediately
- In normal operation: Issues a PANIC-level error message, which will cause the PostgreSQL backend to terminate

The function serves as a last resort mechanism to prevent infinite loops when waiting for spinlocks that may never be released due to bugs or system failures.

## Parameters / Member Variables
- : Source file name where the stuck spinlock was detected
- : Line number in the source file where detection occurred  
- : Function name where the stuck spinlock was detected (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - fprintf (in S_LOCK_TEST mode)
  - exit (in S_LOCK_TEST mode)
  - elog (in normal mode)
  - PANIC (error level constant)
- Called from (representative examples):
  - [perform_spin_delay](../p/perform_spin_delay.md)

## Notes and Other Information
- This is a static function, only accessible within the s_lock.c compilation unit
- The function provides different behavior for testing vs production environments
- When func parameter is NULL, it defaults to "(unknown)" for error reporting
- PANIC level errors in PostgreSQL typically indicate unrecoverable conditions that require process termination

## Simplified Source

```c
// Simplified version of s_lock_stuck
static void
s_lock_stuck(const char *file, int line, const char *func) {
    // Handle null function name
    if (!func)
        func = "(unknown)";

    // Report stuck spinlock and terminate
#if defined(S_LOCK_TEST)
    // Test mode: print to stderr and exit
    fprintf(stderr, "Stuck spinlock detected at %s, %s:%d.\n",
            func, file, line);
    exit(1);
#else
    // Production mode: issue PANIC error (terminates backend)
    elog(PANIC, "stuck spinlock detected at %s, %s:%d",
         func, file, line);
#endif
}
```

Key simplifications made:
- Added clear comments explaining the two execution paths (test vs production)
- Simplified the conditional compilation structure for better readability
- Maintained the essential error reporting and termination logic
- Preserved the null function name handling
- Used clearer formatting for the error message parameters