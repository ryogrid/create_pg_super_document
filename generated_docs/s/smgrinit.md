# smgrinit

## Location
[src/backend/storage/smgr/smgr.c:154-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L154-L171)

## Overview
Initializes all storage managers during backend startup and registers the shutdown cleanup function.

## Definition
```c
void smgrinit(void)
```

## Detailed Description
The `smgrinit` function is responsible for initializing all storage managers in the PostgreSQL storage system. It is called during backend startup (both normal and standalone cases), but not during postmaster start, ensuring that any resources created are backend-local. The function iterates through all available storage managers in the `smgrsw` array and calls their respective initialization functions if they exist. Additionally, it registers the `smgrshutdown` function to be called during process exit to ensure proper cleanup of storage manager resources.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [on_proc_exit](../o/on_proc_exit.md)
  - [smgrshutdown](smgrshutdown.md)
- Called from (representative examples):
  - [BaseInit](../B/BaseInit.md) (src/backend/utils/init/postinit.c:672)

## Notes and Other Information
- This function is called once per backend process during initialization
- Resources created here are backend-local, not shared across the entire PostgreSQL instance
- The function ensures proper cleanup by registering smgrshutdown as an exit handler
- Each storage manager in the smgrsw array may have its own initialization routine

## Simplified Source

```c
// Simplified version of smgrinit
void smgrinit(void) {
    // Initialize all storage managers
    for (int i = 0; i < NSmgr; i++) {
        if (smgrsw[i].smgr_init) {
            smgrsw[i].smgr_init();
        }
    }

    // Register cleanup function to run at process exit
    on_proc_exit(smgrshutdown, 0);
}
```

Key simplifications made:
- Consolidated variable declaration within the for loop
- Added clear comments explaining each major step
- Maintained the essential logic: iterate through storage managers and initialize them
- Preserved the cleanup registration mechanism