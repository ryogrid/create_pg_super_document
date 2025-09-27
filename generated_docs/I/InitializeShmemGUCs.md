# InitializeShmemGUCs

## Location
[src/backend/storage/ipc/ipci.c:369-398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipci.c#L369-L398)

## Overview
Initializes runtime-computed GUC (Grand Unified Configuration) parameters related to shared memory requirements for the current PostgreSQL configuration.

## Definition

```c
void
InitializeShmemGUCs(void)
```
## Detailed Description
This function calculates and sets two important GUC parameters that provide information about shared memory requirements:

1. **shared_memory_size**: The total shared memory size in megabytes, calculated by calling  and rounding up to the nearest megabyte.

2. **shared_memory_size_in_huge_pages**: The number of huge pages required to accommodate the shared memory, calculated only when huge pages are available on the system.

The function operates by:
- Computing the exact shared memory size needed using 
- Converting the byte size to megabytes with proper rounding
- Setting the  GUC parameter
- Checking if huge pages are available via 
- If huge pages are supported, calculating the required number of huge pages and setting the  GUC parameter

These GUC parameters are set with  context and  source, indicating they are internal parameters computed dynamically at startup.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - : Calculates the total shared memory size needed
  - : Safe size addition with overflow checking
  - : Sets a GUC parameter value
  - : Retrieves the system's huge page size
  - : GUC context constant for internal parameters
  - : GUC source constant for dynamically computed defaults

- Called from (representative examples):
  - : Main postmaster initialization in src/backend/postmaster/postmaster.c:942
  - : Single-user mode initialization in src/backend/tcop/postgres.c:4197

## Notes and Other Information
- The function is part of the shared memory initialization process and must be called during PostgreSQL startup
- The computed GUC values provide visibility into memory requirements for monitoring and debugging purposes
- The huge pages calculation is conditional - it only sets the parameter when huge pages are available on the system
- Both GUC parameters are read-only internal parameters that cannot be modified by users
- The shared memory size is rounded up to the nearest megabyte for the GUC display, but the actual allocation uses the precise byte count
- Located in src/backend/storage/ipc/ipci.c:369-398

## Simplified Source

```c
// Simplified version of InitializeShmemGUCs
void InitializeShmemGUCs(void) {
    char buf[64];
    Size total_shared_memory_bytes;
    Size shared_memory_mb;
    Size huge_page_size;

    // Step 1: Calculate total shared memory size and convert to MB
    total_shared_memory_bytes = CalculateShmemSize(NULL);
    shared_memory_mb = (total_shared_memory_bytes + (1024 * 1024) - 1) / (1024 * 1024);

    // Step 2: Set the shared_memory_size GUC parameter
    sprintf(buf, "%zu", shared_memory_mb);
    SetConfigOption("shared_memory_size", buf, PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);

    // Step 3: Calculate and set huge pages requirement (if huge pages available)
    GetHugePageSize(&huge_page_size, NULL);
    if (huge_page_size != 0) {
        Size huge_pages_needed = (total_shared_memory_bytes / huge_page_size) + 1;
        sprintf(buf, "%zu", huge_pages_needed);
        SetConfigOption("shared_memory_size_in_huge_pages", buf, PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);
    }
}
```

Key simplifications made:
- Used more descriptive variable names (total_shared_memory_bytes, shared_memory_mb, huge_pages_needed)
- Replaced add_size() with direct arithmetic for clarity (assuming no overflow in this context)
- Added step-by-step comments explaining the main logic flow
- Consolidated the huge page calculation into a single expression
- Removed detailed comments that were already covered in the documentation above