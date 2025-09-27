# SUBTRANSShmemSize

## Location
[src/backend/access/transam/subtrans.c:214-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L214-L219)

## Overview
SUBTRANSShmemSize calculates the amount of shared memory required for the SUBTRANS (subtransaction) system in PostgreSQL.

## Definition

```c
Size
SUBTRANSShmemSize(void)
```
## Detailed Description
This function computes the total shared memory size needed for the SUBTRANS system, which tracks the commit status of subtransactions. The SUBTRANS system uses a Simple LRU (SLRU) buffer management scheme to cache pages of subtransaction status data. The function delegates the actual calculation to SimpleLruShmemSize, passing the number of buffers determined by SUBTRANSShmemBuffers() and 0 for the number of LSNs (since SUBTRANS doesn't track LSNs per page).

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruShmemSize](SimpleLruShmemSize.md)
  - [SUBTRANSShmemBuffers](SUBTRANSShmemBuffers.md)
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md)

## Notes and Other Information
- Part of the SUBTRANS subsystem that manages subtransaction commit status
- The memory allocation is based on the configured or auto-tuned number of subtransaction buffers
- Used during PostgreSQL startup to determine total shared memory requirements
- Located in src/backend/access/transam/subtrans.c:214-219

## Simplified Source

```c
// Simplified version of SUBTRANSShmemSize
Size SUBTRANSShmemSize(void) {
    // Calculate shared memory size for SUBTRANS system
    // Uses Simple LRU buffer management with configured buffer count
    return SimpleLruShmemSize(SUBTRANSShmemBuffers(), 0);
}
```

Key simplifications made:
- Added explanatory comments for the core purpose
- The function is already quite simple - it delegates to SimpleLruShmemSize
- Highlighted that it uses Simple LRU buffer management
- Noted that the second parameter (0) means no LSN tracking for SUBTRANS