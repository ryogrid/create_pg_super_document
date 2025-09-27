# CreateSharedMemoryAndSemaphores

## Location
[src/backend/storage/ipc/ipci.c:199-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipci.c#L199-L280)

## Overview
Creates and initializes the entire shared memory segment and semaphore infrastructure required for PostgreSQL operation.

## Definition

```c
void
CreateSharedMemoryAndSemaphores(void)
```
## Detailed Description
CreateSharedMemoryAndSemaphores is the central orchestrator for establishing PostgreSQL's shared memory infrastructure. This function is exclusively called by the postmaster process during startup and performs a comprehensive initialization sequence.

The function begins by calculating the required shared memory size and semaphore count via CalculateShmemSize(), then creates the shared memory segment using PGSharedMemoryCreate(). It initializes shared memory access mechanisms and creates the necessary semaphores. For platforms without hardware spinlocks, it initializes the semaphore-based spinlock emulation layer.

The function then sets up the shared memory allocation framework and calls CreateOrAttachShmemStructs() to initialize all PostgreSQL subsystem structures. On EXEC_BACKEND platforms, it allocates the backend tracking array. Finally, it initializes dynamic shared memory facilities and provides a hook for extensions to perform their shared memory initialization.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [CalculateShmemSize](CalculateShmemSize.md) (calculates memory and semaphore requirements)
  - [PGSharedMemoryCreate](../P/PGSharedMemoryCreate.md) (creates the shared memory segment)
  - [GetConfigOption](../G/GetConfigOption.md) (validates huge pages configuration)
  - [InitShmemAccess](../I/InitShmemAccess.md) (initializes shared memory access)
  - [PGReserveSemaphores](../P/PGReserveSemaphores.md) (creates semaphore resources)
  - [SpinlockSemaInit](../S/SpinlockSemaInit.md) (initializes spinlock emulation if needed)
  - [InitShmemAllocation](../I/InitShmemAllocation.md) (sets up memory allocation framework)
  - [CreateOrAttachShmemStructs](CreateOrAttachShmemStructs.md) (initializes subsystem structures)
  - [ShmemBackendArrayAllocation](../S/ShmemBackendArrayAllocation.md) (EXEC_BACKEND backend tracking)
  - [dsm_postmaster_startup](../d/dsm_postmaster_startup.md) (dynamic shared memory initialization)
  - shmem_startup_hook (extension initialization hook)
- Called from (representative examples):
  - [BootstrapModeMain](../B/BootstrapModeMain.md) (bootstrap database creation)
  - [PostmasterMain](../P/PostmasterMain.md) (normal server startup)
  - [PostgresSingleUserMain](../P/PostgresSingleUserMain.md) (single-user mode)

## Notes and Other Information
- Only called by the postmaster process (Assert(!IsUnderPostmaster))
- Must be called before any child processes are spawned
- Validates huge pages configuration during initialization
- Conditionally initializes spinlock emulation on platforms without hardware spinlocks
- Includes EXEC_BACKEND-specific backend array allocation
- Provides extension hook for modules to initialize shared memory structures
- Critical for establishing the foundation of PostgreSQL's inter-process communication

## Simplified Source

```c
// Simplified version of CreateSharedMemoryAndSemaphores
void CreateSharedMemoryAndSemaphores(void) {
    PGShmemHeader *shim;
    PGShmemHeader *seghdr;
    Size size;
    int numSemas;

    // Ensure this is only called by postmaster, not child processes
    Assert(!IsUnderPostmaster);

    // Step 1: Calculate required shared memory size and semaphore count
    size = CalculateShmemSize(&numSemas);
    elog(DEBUG3, "invoking IpcMemoryCreate(size=%zu)", size);

    // Step 2: Create the shared memory segment
    seghdr = PGSharedMemoryCreate(size, &shim);

    // Step 3: Validate huge pages configuration
    Assert(strcmp("unknown", GetConfigOption("huge_pages_status", false, false)) != 0);

    // Step 4: Initialize shared memory access mechanisms
    InitShmemAccess(seghdr);

    // Step 5: Create semaphores for synchronization
    PGReserveSemaphores(numSemas);

    // Step 6: Initialize spinlock emulation if hardware spinlocks unavailable
#ifndef HAVE_SPINLOCKS
    SpinlockSemaInit();
#endif

    // Step 7: Set up shared memory allocation framework
    InitShmemAllocation();

    // Step 8: Initialize all PostgreSQL subsystem shared memory structures
    CreateOrAttachShmemStructs();

    // Step 9: Platform-specific backend tracking (Windows)
#ifdef EXEC_BACKEND
    ShmemBackendArrayAllocation();
#endif

    // Step 10: Initialize dynamic shared memory facilities
    dsm_postmaster_startup(shim);

    // Step 11: Allow extensions to initialize their shared memory
    if (shmem_startup_hook)
        shmem_startup_hook();
}
```

Key simplifications made:
- Added step-by-step comments explaining the logical flow
- Preserved all essential function calls and logic
- Simplified complex comment blocks into concise explanations
- Maintained conditional compilation directives as they're essential
- Focused on the main execution path while keeping error checks
- Made the initialization sequence more apparent through numbered steps