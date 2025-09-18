# on_exit_nicely

## Location
src/bin/pg_dump/pg_backup_utils.c: 63 - 89

## Overview
Registers callback functions to be executed when the program exits via exit_nicely, providing cleanup functionality for pg_dump utilities.

## Definition
```c
void on_exit_nicely(on_exit_nicely_callback function, void *arg)
```

## Detailed Description
This function maintains a registry of callback functions that will be executed in reverse order when exit_nicely is called. It provides a cleanup mechanism for pg_dump and pg_restore utilities, allowing different components to register cleanup handlers that will be called before program termination. The callbacks are stored in a static array with a maximum capacity defined by MAX_ON_EXIT_NICELY.

The function adds new callbacks to the end of the list and increments the index counter. If the maximum number of callbacks is exceeded, the program terminates with a fatal error.

## Parameters / Member Variables
- `function`: Callback function pointer of type on_exit_nicely_callback that will be called on exit
- `arg`: Void pointer to arbitrary data that will be passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - MAX_ON_EXIT_NICELY (constant: 20)
  - pg_fatal (for error handling when callback slots are exhausted)
- Called from (representative examples):
  - on_exit_close_archive (parallel.c:331)

## Notes and Other Information
- Callbacks are executed in reverse order (LIFO - Last In, First Out) by exit_nicely
- Maximum of 20 callbacks can be registered (MAX_ON_EXIT_NICELY)
- The callback signature is: void (*on_exit_nicely_callback)(int code, void *arg)
- Used primarily for cleanup operations like closing files, releasing resources, etc.
- Part of the graceful shutdown mechanism for pg_dump utilities