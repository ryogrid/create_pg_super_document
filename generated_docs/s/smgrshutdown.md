# smgrshutdown

## Location
src/backend/storage/smgr/smgr.c: 172 - 197

## Overview
Cleans up and shuts down all storage managers during backend process termination as a process exit hook.

## Definition
```c
static void smgrshutdown(int code, Datum arg)
```

## Detailed Description
The `smgrshutdown` function serves as a cleanup handler that is registered with the process exit mechanism during `smgrinit`. It is called automatically when the backend process terminates, ensuring that all storage managers have the opportunity to perform their shutdown procedures. The function iterates through all storage managers in the `smgrsw` array and calls their respective shutdown functions if they exist, providing a clean and orderly shutdown of storage management resources.

## Parameters / Member Variables
- `code`: Exit code passed by the process exit handler (not used in this function)
- `arg`: Additional argument passed by the exit handler (not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - SMgrRelation (referenced in the broader context)
- Called from (representative examples):
  - smgrinit (registered as exit handler)

## Notes and Other Information
- This function is declared as static, making it internal to the smgr.c module
- It follows the signature required for `on_proc_exit` callbacks
- Each storage manager may implement its own cleanup routine that gets called during shutdown
- The function ensures proper resource cleanup even if the backend process terminates unexpectedly