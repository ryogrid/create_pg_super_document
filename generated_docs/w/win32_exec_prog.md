# win32_exec_prog

## Location
[src/bin/pg_upgrade/parallel.c:153-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/parallel.c#L153-L171)

## Overview
Windows-specific thread entry point function that executes commands in parallel threads as part of PostgreSQL's pg_upgrade utility.

## Definition

```c
DWORD
win32_exec_prog(exec_thread_arg *args)
```
## Detailed Description
This function serves as the thread entry point for Windows-based parallel execution in pg_upgrade. It's a thin wrapper around exec_prog that adapts the function signature to be compatible with Windows threading APIs (_beginthreadex). The function extracts command parameters from the thread argument structure and executes the command using the standard exec_prog function.

The function inverts the return value from exec_prog (using logical NOT) to match Windows thread return conventions, where 0 typically indicates success.

## Parameters / Member Variables
- : Pointer to exec_thread_arg structure containing:
  - : Path to the main log file
  - : Optional path to additional log file
  - : Command string to execute

## Dependencies
- Functions called/Symbols referenced:
  - [exec_prog](../e/exec_prog.md)
  - [exec_thread_arg](../e/exec_thread_arg.md) (struct type)
- Called from (representative examples):
  - [parallel_exec_prog](../p/parallel_exec_prog.md)
  - transfer_thread_arg

## Notes and Other Information
- Platform-specific: Only compiled and used on Windows platforms
- Threading: Designed as a thread entry point for _beginthreadex()
- Return value: Returns DWORD (0 for success, non-zero for failure) following Windows threading conventions
- Memory management: Does not manage the args structure - assumes it's managed by the calling thread
- Thread lifecycle: Function return terminates the thread

## Simplified Source

```c
DWORD
win32_exec_prog(exec_thread_arg *args)
{
    // Execute the command using standard exec_prog function
    // Invert return value to match Windows threading conventions
    int ret = !exec_prog(args->log_file, args->opt_log_file, true, true, "%s", args->cmd);

    // Return terminates the thread
    return ret;
}
```