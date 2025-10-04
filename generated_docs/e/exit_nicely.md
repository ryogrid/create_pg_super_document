# exit_nicely

## Location
[src/bin/pg_dump/pg_backup_utils.c:90-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_utils.c#L90-L104)

## Overview
Executes registered cleanup callbacks in reverse order and then terminates the program, providing graceful shutdown for pg_dump utilities.

## Definition
```c
void exit_nicely(int code) pg_attribute_noreturn()
```

## Detailed Description
This function provides a controlled program termination mechanism for pg_dump and related utilities. It runs all previously registered cleanup callbacks (via on_exit_nicely) in reverse order (LIFO), allowing proper cleanup of resources, file handles, and other program state before exiting.

The function handles both single-threaded and multi-threaded scenarios. On Windows in parallel operation mode, it distinguishes between the main thread and worker threads - worker threads exit only the thread (using _endthreadex), while the main thread exits the entire process. On Unix systems, the function always exits the entire process.

The callback execution order (reverse of registration) ensures that cleanup operations occur in the proper dependency order, with the most recently registered callbacks (typically the most specific/dependent resources) being cleaned up first.

## Parameters / Member Variables
- `code`: Exit code to be passed to both callbacks and the final exit() call

## Dependencies
- Functions called/Symbols referenced:
  - on_exit_nicely_list (static array of callback structures)
  - on_exit_nicely_index (static counter)
  - exit() (standard library function)
  - _endthreadex() (Windows-specific thread exit function)
- Called from (representative examples):
  - [set_dump_section](../s/set_dump_section.md) (pg_backup_utils.c:56)
  - [pg_fatal](../p/pg_fatal.md) (macro in pg_backup_utils.h:38)
  - [main](../m/main.md) functions in pg_dump.c, pg_restore.c, pg_dumpall.c
  - Various error handling locations throughout pg_dump utilities

## Notes and Other Information
- Marked with pg_attribute_noreturn() indicating it never returns to caller
- Callbacks receive both the exit code and their registered argument
- On Windows parallel mode, only worker threads use _endthreadex(), main thread uses exit()
- Thread-safety considerations: callback list is shared between threads on Windows
- Each callback should contain logic appropriate for its execution context (thread vs process)
- Callbacks should be registered before forking child processes to maintain consistency
- Used extensively throughout pg_dump utilities for error handling and normal termination

## Simplified Source

```c
void
exit_nicely(int code)
{
    int i;

    // Run cleanup callbacks in reverse order (LIFO)
    for (i = on_exit_nicely_index - 1; i >= 0; i--)
        on_exit_nicely_list[i].function(code, on_exit_nicely_list[i].arg);

#ifdef WIN32
    // On Windows: worker threads exit only the thread, main thread exits process
    if (parallel_init_done && GetCurrentThreadId() != mainThreadId)
        _endthreadex(code);
#endif

    // Exit the process
    exit(code);
}
```