# exec_thread_arg

## Location
[src/bin/pg_upgrade/parallel.c:34-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/parallel.c#L34-L42)

## Overview
A structure that holds arguments required for executing commands in parallel threads on Windows platforms during PostgreSQL upgrade operations.

## Definition

```c
typedef struct
{
	DbInfoArr  *old_db_arr;
	DbInfoArr  *new_db_arr;
	char	   *old_pgdata;
	char	   *new_pgdata;
	char	   *old_tablespace;
} transfer_thread_arg;
```
## Detailed Description
The  structure is used specifically in the  utility to facilitate parallel command execution on Windows platforms. This structure encapsulates the necessary parameters for thread-based command execution, allowing the upgrade process to run multiple operations concurrently for improved performance. The structure is part of PostgreSQL's parallel execution infrastructure within the upgrade utility, where it serves as a communication mechanism between the main thread and worker threads executing database upgrade commands.

On Windows systems, when parallel jobs are enabled (user_opts.jobs > 1), this structure is allocated and populated with command execution parameters, then passed to Windows threads via the  function. The structure lifetime is managed carefully to avoid cross-thread memory issues - arguments are allocated during the entire process lifetime and freed only in the same thread that allocated them.

## Parameters / Member Variables
- : Path to the primary log file where command output and messages should be written
- : Path to an optional secondary log file for additional logging output
- : The actual command string to be executed by the thread

## Dependencies
- Functions called/Symbols referenced:
  - (This structure is primarily a data container and doesn't directly call functions)
- Called from (representative examples):
  -  (allocates and populates the structure)
  -  (consumes the structure as a parameter)

## Notes and Other Information
- This structure is Windows-specific () and is not used on Unix-like platforms
- Memory for  instances is allocated in arrays () with one slot per parallel job
- The structure is designed for thread safety by avoiding cross-thread memory operations
- Used in conjunction with Windows thread handles ( array) for parallel job management
- Part of the broader parallel execution framework in  that includes similar structures like  for different types of parallel operations