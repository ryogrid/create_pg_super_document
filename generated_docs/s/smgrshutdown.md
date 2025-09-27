# smgrshutdown

## Location
[src/backend/storage/smgr/smgr.c:172-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L172-L197)

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
  - [smgrinit](smgrinit.md) (registered as exit handler)

## Notes and Other Information
- This function is declared as static, making it internal to the smgr.c module
- It follows the signature required for `on_proc_exit` callbacks
- Each storage manager may implement its own cleanup routine that gets called during shutdown
- The function ensures proper resource cleanup even if the backend process terminates unexpectedly

## Simplified Source

```c
// Simplified version of smgrshutdown
static void smgrshutdown(int code, Datum arg) {
    int i;

    // Call shutdown function for each storage manager
    for (i = 0; i < NSmgr; i++) {
        if (smgrsw[i].smgr_shutdown) {
            smgrsw[i].smgr_shutdown();
        }
    }
}
```

Key simplifications made:
- Simplified comment to focus on core purpose
- Maintained essential loop structure and logic
- Preserved NULL check for shutdown function pointer
- Kept all critical cleanup operations